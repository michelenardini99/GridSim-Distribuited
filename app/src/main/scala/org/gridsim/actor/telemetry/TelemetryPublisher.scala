package org.gridsim.actor.telemetry

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.gridsim.core.common.{Energy, Flow}
import org.gridsim.core.model.{Environment, GridEntityState}
import org.gridsim.core.model.network.Cable
import org.gridsim.core.observability.serialization.{SimulationControlCodec, SimulationControlUpdate, SimulationDataCodecs}

import java.util.Properties
import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*

/** Publisher interface allowing an [[org.gridsim.actor.EntityActor]] to push its
  * own state and energy flow telemetry directly to Kafka at each simulation tick.
  */
trait EntityTelemetryPublisher:
  def publish(entityId: String, tick: Long, state: GridEntityState, flow: Flow[Energy]): Unit
  def close(): Unit = ()

/** Publisher interface allowing the [[org.gridsim.actor.SimulationActor]] to push
  * global simulation progress, environment, and cable load distribution to Kafka.
  */
trait SimulationTickPublisher:
  def publish(tick: Long, env: Environment, cableLoads: Map[Cable, Energy], delta: FiniteDuration): Unit
  def close(): Unit = ()

/** Publisher interface allowing the [[org.gridsim.actor.SimulationActor]] to broadcast
  * lifecycle and configuration changes (status, speed, tick delta) to every client.
  */
trait SimulationControlPublisher:
  def publish(update: SimulationControlUpdate): Unit
  def close(): Unit = ()

/** Production [[EntityTelemetryPublisher]] backed by an Apache Kafka producer.
  * Emits [[org.gridsim.protocol.v1.entity_state.EntityTelemetryMessage]] binary payloads
  * keyed by `entityId` to ensure per-entity partition ordering.
  */
class KafkaEntityTelemetryPublisher(
    producer: Producer[String, Array[Byte]],
    topic: String = "grid.entities"
) extends EntityTelemetryPublisher:

  override def publish(
      entityId: String,
      tick: Long,
      state: GridEntityState,
      flow: Flow[Energy]
  ): Unit =
    val bytes = SimulationDataCodecs.telemetryToBinary(entityId, tick, state, flow)
    val record = new ProducerRecord[String, Array[Byte]](topic, entityId, bytes)
    producer.send(record)

  override def close(): Unit = producer.close()

/** Production [[SimulationTickPublisher]] backed by an Apache Kafka producer.
  * Emits [[org.gridsim.protocol.v1.simulation_data.GridTickMessage]] binary payloads.
  */
class KafkaSimulationTickPublisher(
    producer: Producer[String, Array[Byte]],
    topic: String = "grid.ticks"
) extends SimulationTickPublisher:

  override def publish(
      tick: Long,
      env: Environment,
      cableLoads: Map[Cable, Energy],
      delta: FiniteDuration
  ): Unit =
    val bytes = SimulationDataCodecs.gridTickToBinary(tick, env, cableLoads, delta)
    val record = new ProducerRecord[String, Array[Byte]](topic, tick.toString, bytes)
    producer.send(record)

  override def close(): Unit = producer.close()

/** Production [[SimulationControlPublisher]] backed by an Apache Kafka producer.
  * All updates share the same key so they land on one partition and keep their order.
  */
class KafkaSimulationControlPublisher(
    producer: Producer[String, Array[Byte]],
    simulationId: String,
    topic: String
) extends SimulationControlPublisher:

  override def publish(update: SimulationControlUpdate): Unit =
    val record = new ProducerRecord[String, Array[Byte]](topic, simulationId, SimulationControlCodec.toBinary(update))
    producer.send(record)

  override def close(): Unit = producer.close()

object TelemetryPublisher:

  /** Helper to construct a standard [[KafkaProducer]] configured for byte array values. */
  def createKafkaProducer(bootstrapServers: String): KafkaProducer[String, Array[Byte]] =
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "5")
    new KafkaProducer[String, Array[Byte]](props)

  /** No-op implementation for environments where Kafka is not enabled or needed. */
  object NoOpEntityTelemetryPublisher extends EntityTelemetryPublisher:
    override def publish(entityId: String, tick: Long, state: GridEntityState, flow: Flow[Energy]): Unit = ()

  object NoOpSimulationTickPublisher extends SimulationTickPublisher:
    override def publish(tick: Long, env: Environment, cableLoads: Map[Cable, Energy], delta: FiniteDuration): Unit = ()

  object NoOpSimulationControlPublisher extends SimulationControlPublisher:
    override def publish(update: SimulationControlUpdate): Unit = ()

  /** Thread-safe recording publisher used in automated unit and actor tests. */
  class RecordingEntityTelemetryPublisher extends EntityTelemetryPublisher:
    case class Record(entityId: String, tick: Long, state: GridEntityState, flow: Flow[Energy])
    private val buffer = new ConcurrentLinkedQueue[Record]()

    override def publish(entityId: String, tick: Long, state: GridEntityState, flow: Flow[Energy]): Unit =
      buffer.add(Record(entityId, tick, state, flow))

    def records: List[Record] = buffer.asScala.toList
    def clear(): Unit = buffer.clear()

  /** Thread-safe recording tick publisher used in automated unit and actor tests. */
  class RecordingSimulationTickPublisher extends SimulationTickPublisher:
    case class Record(tick: Long, env: Environment, cableLoads: Map[Cable, Energy], delta: FiniteDuration)
    private val buffer = new ConcurrentLinkedQueue[Record]()

    override def publish(tick: Long, env: Environment, cableLoads: Map[Cable, Energy], delta: FiniteDuration): Unit =
      buffer.add(Record(tick, env, cableLoads, delta))

    def records: List[Record] = buffer.asScala.toList
    def clear(): Unit = buffer.clear()
