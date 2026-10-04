package org.gridsim.integration.telemetry

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityRef
import org.apache.pekko.cluster.sharding.typed.testkit.scaladsl.TestEntityRef
import org.apache.pekko.persistence.testkit.scaladsl.EventSourcedBehaviorTestKit
import org.apache.pekko.persistence.typed.PersistenceId
import org.gridsim.actor.{EntityActor, SimulationActor}
import org.gridsim.actor.protocol.EntityProtocol
import org.gridsim.actor.protocol.EntityProtocol.{EntityCommand, EntityEvent, EntityEvolved, Evolve}
import org.gridsim.actor.protocol.SimulationProtocol
import org.gridsim.actor.protocol.SimulationProtocol.{EntitiesEvolved, SimulationCommand, SimulationEvent, Start, TickAdvanced}
import org.gridsim.actor.telemetry.{KafkaEntityTelemetryPublisher, KafkaSimulationTickPublisher, TelemetryPublisher}
import org.gridsim.core.behaviour.{EntityEvolutionHandler, EvolutionRequest}
import org.gridsim.core.common.*
import org.gridsim.core.common.Energy.*
import org.gridsim.core.common.Flow.*
import org.gridsim.core.common.Power.*
import org.gridsim.core.model.*
import org.gridsim.core.model.network.{Cable, CableConnections, GridGraph}
import org.gridsim.core.model.storage.battery.BatteryState
import org.gridsim.core.observability.serialization.SimulationDataCodecs
import org.gridsim.core.simulation.{SimulationConf, SimulationModel, SimulationState}
import org.gridsim.core.solver.PowerFlowSolver
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import java.net.{InetSocketAddress, Socket}
import java.time.LocalDateTime
import java.util.{Collections, Properties, UUID}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** End-to-end integration test validating the Hybrid Telemetry Model against a live Apache Kafka broker.
  *
  * Verifies:
  *   1. [[EntityActor]] emits binary Protobuf [[org.gridsim.protocol.v1.entity_state.EntityTelemetryMessage]]
  *      records to `grid.entities` keyed by `entityId`.
  *   2. [[SimulationActor]] emits binary Protobuf [[org.gridsim.protocol.v1.simulation_data.GridTickMessage]]
  *      records to `grid.ticks` keyed by `tick`.
  *   3. Deserialization via [[SimulationDataCodecs]] reconstructs identical domain models.
  *   4. Gracefully skips via `assume` if Kafka is not available on localhost:9092.
  */
@RunWith(classOf[JUnitRunner])
class KafkaTelemetryIntegrationSpec
    extends ScalaTestWithActorTestKit(EventSourcedBehaviorTestKit.config)
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  private val BootstrapServers = "localhost:9092"

  private def isKafkaReachable(): Boolean =
    try
      val socket = new Socket()
      socket.connect(new InetSocketAddress("127.0.0.1", 9092), 1000)
      socket.close()
      true
    catch
      case _: Exception => false

  private lazy val producer =
    if isKafkaReachable() then TelemetryPublisher.createKafkaProducer(BootstrapServers)
    else null

  override def afterAll(): Unit =
    if producer != null then producer.close()
    super.afterAll()

  private def createConsumer(): KafkaConsumer[String, Array[Byte]] =
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, s"test-group-${UUID.randomUUID()}")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    new KafkaConsumer[String, Array[Byte]](props)

  private def pollRecords[K, V](
      consumer: KafkaConsumer[K, V],
      expectedCount: Int,
      timeout: FiniteDuration = 10.seconds
  ): List[ConsumerRecord[K, V]] =
    val deadline = System.currentTimeMillis() + timeout.toMillis
    val buffer = collection.mutable.ListBuffer[ConsumerRecord[K, V]]()
    while buffer.size < expectedCount && System.currentTimeMillis() < deadline do
      val polled = consumer.poll(java.time.Duration.ofMillis(200))
      buffer ++= polled.asScala
    buffer.toList

  private case class DummyEntity(id: String) extends GridEntity

  private val env = Environment(LocalDateTime.of(2026, 10, 2, 12, 0, 0), 0.seconds)
  private val delta = 1.minute
  private val cable = Cable(CableConnections("solar-1", "grid-hub"), maxCapacity = 100.kw)

  "KafkaEntityTelemetryPublisher and EntityActor" should "publish binary Protobuf telemetry directly to Kafka" in {
    assume(isKafkaReachable(), s"Skipping test: Kafka broker not reachable on $BootstrapServers")

    val entityTopic = s"grid.entities.test.${UUID.randomUUID()}"
    val entityPublisher = new KafkaEntityTelemetryPublisher(producer, entityTopic)

    val entity = DummyEntity("solar-1")
    val initialSolarState = SolarPanelState("solar-1", efficiency = 0.20)
    val evolvedSolarState = SolarPanelState("solar-1", efficiency = 0.22)
    val flowResult: Flow[Energy] = Surplus(12.5.kwh)

    val handler = new EntityEvolutionHandler:
      override def supports(request: EvolutionRequest): Boolean = true
      override def evolve(request: EvolutionRequest): (GridEntityState, Flow[Energy]) =
        (evolvedSolarState, flowResult)

    val kit = EventSourcedBehaviorTestKit[EntityCommand, EntityEvent, EntityActor.State](
      testKit.system,
      EntityActor(
        PersistenceId(EntityActor.TypeKey.name, UUID.randomUUID().toString),
        _ => handler,
        entityPublisher
      ),
      EventSourcedBehaviorTestKit.SerializationSettings.disabled
    )

    // 1. Initialize EntityActor
    val initReply = kit.runCommand[EntityProtocol.Ack.type](replyTo => EntityProtocol.Initialize(entity, initialSolarState, replyTo))
    initReply.reply shouldBe EntityProtocol.Ack

    // 2. Evolve EntityActor at tick 5
    val evolveReply = kit.runCommand[EntityEvolved](replyTo => Evolve(env, delta, replyTo, 5L))
    // Lightweight reply: does not contain state
    evolveReply.reply shouldBe EntityEvolved("solar-1", flowResult)

    // 3. Verify Kafka received the Protobuf telemetry record
    val consumer = createConsumer()
    try
      consumer.subscribe(Collections.singletonList(entityTopic))
      val records = pollRecords(consumer, 1)

      records should have size 1
      val record = records.head
      record.key() shouldBe "solar-1"

      val decoded = SimulationDataCodecs.telemetryFromBinary(record.value())
      decoded.isRight shouldBe true

      val (entityId, tick, state, flow) = decoded.toOption.get
      entityId shouldBe "solar-1"
      tick shouldBe 5L
      state shouldBe evolvedSolarState
      flow shouldBe flowResult
    finally
      consumer.close()
  }

  "KafkaSimulationTickPublisher and SimulationActor" should "publish global grid tick data directly to Kafka" in {
    assume(isKafkaReachable(), s"Skipping test: Kafka broker not reachable on $BootstrapServers")

    val tickTopic = s"grid.ticks.test.${UUID.randomUUID()}"
    val tickPublisher = new KafkaSimulationTickPublisher(producer, tickTopic)

    val gridGraph = GridGraph(nodes = List(DummyEntity("solar-1")), cables = List(cable))
    val model = SimulationModel(gridGraph)
    val conf = SimulationConf(delta = delta)


    val kit = EventSourcedBehaviorTestKit[SimulationCommand, SimulationEvent, SimulationActor.State](
      testKit.system,
      SimulationActor(
        PersistenceId(SimulationActor.TypeKey.name, UUID.randomUUID().toString),
        id => TestEntityRef(EntityActor.TypeKey, id, testKit.spawn(org.apache.pekko.actor.typed.scaladsl.Behaviors.ignore[EntityCommand])),
        1.hour,
        tickPublisher
      ),
      EventSourcedBehaviorTestKit.SerializationSettings.disabled
    )

    val initState = SimulationState(env, Map.empty, Map.empty, Map.empty)
    kit.runCommand[SimulationProtocol.Ack.type](replyTo => SimulationProtocol.Initialize(model, initState, conf, replyTo))
    kit.runCommand(Start)

    val newEnv = env.advance(delta)
    val evolvedResults = List(EntityEvolved("solar-1", Surplus(12.5.kwh)))
    val res = kit.runCommand(EntitiesEvolved(newEnv, evolvedResults))

    res.event shouldBe TickAdvanced(newEnv, 1L)

    val consumer = createConsumer()
    try
      consumer.subscribe(Collections.singletonList(tickTopic))
      val records = pollRecords(consumer, 1)

      records should have size 1
      val record = records.head
      record.key() shouldBe "0"

      val decoded = SimulationDataCodecs.gridTickFromBinary(record.value())
      decoded.isRight shouldBe true

      val (tick, decodedEnv, cableLoads, decodedDelta) = decoded.toOption.get
      tick shouldBe 0L
      decodedEnv shouldBe newEnv
      cableLoads shouldBe Map(cable -> 12.5.kwh)
      decodedDelta shouldBe delta
    finally
      consumer.close()
  }

  "Hybrid Telemetry Architecture" should "support end-to-end multi-tick and multi-entity telemetry publishing" in {
    assume(isKafkaReachable(), s"Skipping test: Kafka broker not reachable on $BootstrapServers")

    val entityTopic = s"grid.entities.test.${UUID.randomUUID()}"
    val tickTopic = s"grid.ticks.test.${UUID.randomUUID()}"

    val entityPublisher = new KafkaEntityTelemetryPublisher(producer, entityTopic)
    val tickPublisher = new KafkaSimulationTickPublisher(producer, tickTopic)

    // Publish telemetry across 2 ticks for 2 entities
    entityPublisher.publish("node-1", 0L, SolarPanelState("node-1", 0.18), Surplus(10.kwh))
    entityPublisher.publish("node-2", 0L, BatteryState("node-2", 50.kwh), Deficit(10.kwh))
    tickPublisher.publish(0L, env, Map(cable -> 10.kwh), delta)

    val nextEnv = env.advance(delta)
    entityPublisher.publish("node-1", 1L, SolarPanelState("node-1", 0.20), Surplus(15.kwh))
    entityPublisher.publish("node-2", 1L, BatteryState("node-2", 65.kwh), Deficit(15.kwh))
    tickPublisher.publish(1L, nextEnv, Map(cable -> 15.kwh), delta)

    // Consume entity telemetry
    val entityConsumer = createConsumer()
    try
      entityConsumer.subscribe(Collections.singletonList(entityTopic))
      val records = pollRecords(entityConsumer, 4)
      records should have size 4

      val decodedEntities = records.map(r => SimulationDataCodecs.telemetryFromBinary(r.value()).toOption.get)
      decodedEntities.map(t => (t._1, t._2)) shouldBe List(
        ("node-1", 0L),
        ("node-2", 0L),
        ("node-1", 1L),
        ("node-2", 1L)
      )
    finally
      entityConsumer.close()

    // Consume tick telemetry
    val tickConsumer = createConsumer()
    try
      tickConsumer.subscribe(Collections.singletonList(tickTopic))
      val records = pollRecords(tickConsumer, 2)
      records should have size 2

      val decodedTicks = records.map(r => SimulationDataCodecs.gridTickFromBinary(r.value()).toOption.get)
      decodedTicks.map(_._1) shouldBe List(0L, 1L)
      decodedTicks.head._3 shouldBe Map(cable -> 10.kwh)
      decodedTicks.last._3 shouldBe Map(cable -> 15.kwh)
    finally
      tickConsumer.close()
  }
}

