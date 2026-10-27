package org.gridsim.integration.cluster

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Scheduler}
import org.apache.pekko.cluster.MemberStatus
import org.apache.pekko.cluster.sharding.typed.ClusterShardingSettings
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity}
import org.apache.pekko.cluster.typed.{Cluster, ClusterSingleton, SingletonActor}
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.util.Timeout
import org.gridsim.actor.{EntityActor, SimulationActor, SimulationRegistryActor}
import org.gridsim.actor.protocol.EntityProtocol
import org.gridsim.actor.protocol.EntityProtocol.{EntityCommand, EntityEvolved, Evolve}
import org.gridsim.actor.telemetry.{KafkaEntityTelemetryPublisher, KafkaSimulationControlPublisher, KafkaSimulationTickPublisher, TelemetryPublisher}
import org.gridsim.core.behaviour.{EntityEvolutionHandler, EvolutionRequest}
import org.gridsim.core.common.*
import org.gridsim.core.model.{Environment, GridEntity, GridEntityState, SolarPanelState}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import java.net.{InetSocketAddress, Socket}
import java.time.LocalDateTime
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.util.Try

private final case class DummyGridEntity(id: String) extends GridEntity

/** End-to-end chaos/resilience integration tests validating that the production
  * Pekko Cluster wiring (Cluster Sharding, Cluster Singleton master election,
  * Redis-backed persistence, and the keep-majority Split Brain Resolver) actually
  * survives abrupt node crashes.
  *
  * Each test forms a real 3-node in-process cluster (3 distinct [[ActorSystem]]s,
  * on dedicated ports 25520-25522) replicating -- without duplicating 1:1 -- the
  * wiring already present in [[org.gridsim.actor.ClusterMain]]. No production file
  * is modified; this spec only reuses the same classes/config from outside.
  *
  * Gracefully skipped via `assume` if Redis and/or Kafka are not reachable locally
  * (see the `startRedisContainer`/`startKafkaContainer` Gradle tasks).
  */
@RunWith(classOf[JUnitRunner])
class ClusterResilienceIntegrationSpec extends AnyFlatSpecLike with Matchers with BeforeAndAfterEach {

  private val SystemName = "GridSimResilienceTestSystem"
  private val Host = "127.0.0.1"
  private val Node1Port = 25520
  private val Node2Port = 25521
  private val Node3Port = 25522
  private val RedisHost = "127.0.0.1"
  private val RedisPort = 6379
  private val KafkaBootstrapServers = "localhost:9092"


  private val SbrStableAfter = 3.seconds

  private val liveSystems = mutable.ListBuffer.empty[ActorSystem[Nothing]]

  private def isPortOpen(host: String, port: Int, timeoutMs: Int = 1000): Boolean =
    try
      val socket = new Socket()
      socket.connect(new InetSocketAddress(host, port), timeoutMs)
      socket.close()
      true
    catch case _: Exception => false

  private def isRedisReachable(): Boolean = isPortOpen(RedisHost, RedisPort)
  private def isKafkaReachable(): Boolean = isPortOpen("127.0.0.1", 9092)

  private def waitForCondition(timeout: FiniteDuration, interval: FiniteDuration = 200.millis)(condition: => Boolean): Boolean =
    val deadline = System.currentTimeMillis() + timeout.toMillis
    var ok = condition
    while !ok && System.currentTimeMillis() < deadline do
      Thread.sleep(interval.toMillis)
      ok = condition
    ok

  /** Retries `action` until it succeeds (no exception, e.g. no ask-timeout while
    * sharding is rebalancing after a crash) or the timeout elapses.
    */
  private def retryUntilSuccess[T](timeout: FiniteDuration, interval: FiniteDuration = 500.millis)(action: => T): Option[T] =
    val deadline = System.currentTimeMillis() + timeout.toMillis
    var result: Option[T] = None
    while result.isEmpty && System.currentTimeMillis() < deadline do
      result = Try(action).toOption
      if result.isEmpty then Thread.sleep(interval.toMillis)
    result

  private final case class NodeHandle(
    system: ActorSystem[Nothing],
    sharding: ClusterSharding,
    registryProxy: ActorRef[SimulationRegistryActor.Command]
  )

  private def nodeConfig(port: Int, seedPort: Int): Config =
    ConfigFactory
      .parseString(s"""
      pekko.actor.provider = cluster
      pekko.actor.allow-java-serialization = on
      pekko.actor.warn-about-java-serializer-usage = off
      pekko.remote.artery.canonical.hostname = "$Host"
      pekko.remote.artery.canonical.port = $port
      pekko.cluster.seed-nodes = ["pekko://$SystemName@$Host:$seedPort"]
      pekko.cluster.downing-provider-class = "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"
      pekko.cluster.split-brain-resolver.active-strategy = keep-majority
      pekko.cluster.split-brain-resolver.stable-after = ${SbrStableAfter.toSeconds}s
      pekko.cluster.failure-detector.heartbeat-interval = 500ms
      pekko.cluster.failure-detector.acceptable-heartbeat-pause = 1s
      pekko.coordinated-shutdown.run-by-actor-system-terminate = off
      pekko.persistence.journal.plugin = "gridsim.persistence.redis.journal"
      pekko.persistence.snapshot-store.plugin = "gridsim.persistence.redis.snapshot"
      gridsim.persistence.redis.journal {
        class = "org.gridsim.actor.persistence.RedisJournal"
        plugin-dispatcher = "pekko.actor.default-dispatcher"
        host = "$RedisHost"
        port = $RedisPort
      }
      gridsim.persistence.redis.snapshot {
        class = "org.gridsim.actor.persistence.RedisSnapshotStore"
        plugin-dispatcher = "pekko.actor.default-dispatcher"
        host = "$RedisHost"
        port = $RedisPort
      }
    """)
      .withFallback(ConfigFactory.load())

  private val evolutionHandler: GridEntity => EntityEvolutionHandler = _ =>
    new EntityEvolutionHandler:
      override def supports(req: EvolutionRequest): Boolean = true
      override def evolve(req: EvolutionRequest): (GridEntityState, Flow[Energy]) =
        (req.state, Flow.Surplus(42.5.kwh))

  /** Replicates the relevant subset of [[org.gridsim.actor.ClusterMain]]'s wiring
    * (sharding for [[EntityActor]]/[[SimulationActor]] + [[ClusterSingleton]] for
    * the registry) on a freshly started, isolated [[ActorSystem]].
    */
  private def startNode(port: Int, seedPort: Int): NodeHandle =
    val config = nodeConfig(port, seedPort)
    val ready = scala.concurrent.Promise[(ClusterSharding, ActorRef[SimulationRegistryActor.Command])]()
    val kafkaProducer = TelemetryPublisher.createKafkaProducer(KafkaBootstrapServers)

    val system = ActorSystem[Nothing](
      Behaviors.setup[Nothing] { context =>
        implicit val sys: ActorSystem[Nothing] = context.system
        val sharding = ClusterSharding(sys)

        // 1. Initialize EntityActor Sharding (mirrors ClusterMain)
        sharding.init(Entity(EntityActor.TypeKey) { entityContext =>
          EntityActor(
            PersistenceId(entityContext.entityTypeKey.name, entityContext.entityId),
            evolutionHandler,
            new KafkaEntityTelemetryPublisher(kafkaProducer, "grid.entities.resilience-test")
          )
        })

        // 2. Initialize SimulationActor Sharding with rememberEntities (mirrors ClusterMain)
        sharding.init(
          Entity(SimulationActor.TypeKey) { entityContext =>
            val simulationId = entityContext.entityId
            SimulationActor(
              PersistenceId(entityContext.entityTypeKey.name, simulationId),
              localId => sharding.entityRefFor(EntityActor.TypeKey, localId),
              1.second,
              new KafkaSimulationTickPublisher(kafkaProducer, "grid.ticks.resilience-test"),
              new KafkaSimulationControlPublisher(kafkaProducer, simulationId, "grid.control.resilience-test")
            )
          }.withSettings(ClusterShardingSettings(sys).withRememberEntities(true))
        )

        // 3. Elect/connect to the Master SimulationRegistryActor via ClusterSingleton (mirrors ClusterMain)
        val registryProxy = ClusterSingleton(sys).init(
          SingletonActor(SimulationRegistryActor.persistent(), "SimulationRegistry")
        )

        ready.success((sharding, registryProxy))
        Behaviors.empty
      },
      SystemName,
      config
    )

    val (sharding, registryProxy) = Await.result(ready.future, 15.seconds)
    liveSystems.synchronized(liveSystems += system)
    NodeHandle(system, sharding, registryProxy)

  private def askRegistry[Res](node: NodeHandle, timeout: FiniteDuration)(message: ActorRef[Res] => SimulationRegistryActor.Command): Res =
    implicit val to: Timeout = Timeout(timeout)
    implicit val sched: Scheduler = node.system.scheduler
    Await.result(node.registryProxy.ask(message), timeout)

  private def askEntity[Res](node: NodeHandle, entityId: String, timeout: FiniteDuration)(message: ActorRef[Res] => EntityCommand): Res =
    implicit val to: Timeout = Timeout(timeout)
    implicit val sched: Scheduler = node.system.scheduler
    val ref = node.sharding.entityRefFor(EntityActor.TypeKey, entityId)
    Await.result(ref.ask(message), timeout)

  private def allUp(nodes: Seq[NodeHandle], expectedCount: Int): Boolean =
    nodes.forall { n =>
      val state = Cluster(n.system).state
      state.members.size == expectedCount && state.members.forall(_.status == MemberStatus.Up)
    }

  private def terminateAllLiveSystems(): Unit =
    liveSystems.synchronized {
      liveSystems.foreach { system =>
        if !system.whenTerminated.isCompleted then system.terminate()
      }
      liveSystems.foreach(system => Try(Await.result(system.whenTerminated, 15.seconds)))
      liveSystems.clear()
    }

  override def afterEach(): Unit =
    terminateAllLiveSystems()

  private val env = Environment(LocalDateTime.of(2026, 10, 2, 12, 0, 0), 0.seconds)
  private val delta = 1.minute

  "A 3-node Pekko cluster formed with production-equivalent config" should
    "have every node reach MemberStatus.Up in every node's own cluster view" in {
      assume(isRedisReachable(), "Skipping: Redis not reachable on localhost:6379")
      assume(isKafkaReachable(), "Skipping: Kafka not reachable on localhost:9092")

      val node1 = startNode(Node1Port, Node1Port)
      val node2 = startNode(Node2Port, Node1Port)
      val node3 = startNode(Node3Port, Node1Port)
      val nodes = Seq(node1, node2, node3)

      waitForCondition(30.seconds)(allUp(nodes, 3)) shouldBe true
    }

  "ClusterSingleton master election" should
    "survive the abrupt crash of the node hosting the registry, with the Redis-backed registration intact" in {
      assume(isRedisReachable(), "Skipping: Redis not reachable on localhost:6379")
      assume(isKafkaReachable(), "Skipping: Kafka not reachable on localhost:9092")

      val node1 = startNode(Node1Port, Node1Port)
      val node2 = startNode(Node2Port, Node1Port)
      val node3 = startNode(Node3Port, Node1Port)
      val nodes = Seq(node1, node2, node3)
      waitForCondition(30.seconds)(allUp(nodes, 3)) shouldBe true

      val simulationId = UUID.randomUUID().toString

      val registered = askRegistry[SimulationRegistryActor.Registered](node1, 10.seconds)(replyTo =>
        SimulationRegistryActor.RegisterSimulation(simulationId, "preset-A", replyTo)
      )
      registered shouldBe SimulationRegistryActor.Registered(simulationId)

      // Crash node1 abruptly: no graceful cluster leave, indistinguishable from a hard failure.
      node1.system.terminate()

      // From a survivor, the singleton must be re-elected and the Redis-backed state recovered.
      val recovered = retryUntilSuccess(30.seconds) {
        askRegistry[SimulationRegistryActor.SimulationsList](node2, 5.seconds)(replyTo =>
          SimulationRegistryActor.GetSimulations(replyTo)
        )
      }

      recovered.map(_.simulations.exists(_.id == simulationId)) shouldBe Some(true)
    }

  "Cluster Sharding with Redis-backed persistence" should
    "serve the cached evolution result for an already-processed tick even after the hosting node crashes" in {
      assume(isRedisReachable(), "Skipping: Redis not reachable on localhost:6379")
      assume(isKafkaReachable(), "Skipping: Kafka not reachable on localhost:9092")

      val node1 = startNode(Node1Port, Node1Port)
      val node2 = startNode(Node2Port, Node1Port)
      val node3 = startNode(Node3Port, Node1Port)
      val nodes = Seq(node1, node2, node3)
      waitForCondition(30.seconds)(allUp(nodes, 3)) shouldBe true

      val entityId = UUID.randomUUID().toString
      val entity = DummyGridEntity(entityId)
      val initialState = SolarPanelState(entityId, efficiency = 0.2)

      // Generous timeout: this is the very first message ever routed through this
      // freshly-started shard region (shard allocation + entity activation + first
      // Redis round-trip all happen on this call).
      askEntity[EntityProtocol.Ack.type](node1, entityId, 30.seconds)(replyTo =>
        EntityProtocol.Initialize(entity, Some(initialState), replyTo)
      ) shouldBe EntityProtocol.Ack

      val tick = 1L
      val firstEvolved = askEntity[EntityEvolved](node1, entityId, 20.seconds)(replyTo => Evolve(env, delta, replyTo, tick))
      firstEvolved shouldBe EntityEvolved(entityId, Flow.Surplus(42.5.kwh))

      // Crash the node that originally hosted the shard for this entity.
      node1.system.terminate()

      // Resend the SAME tick from a survivor. Cluster Sharding must lazily re-host the
      // entity on a live node; EntityActor recovers `lastTick`/`lastFlow` from Redis and,
      // per its idempotency check (`tick <= lastTick.get`), returns the cached flow instead
      // of falling back to `Flow.Surplus(Energy.Zero)` from an empty, unrecovered state.
      val replayed = retryUntilSuccess(45.seconds, interval = 1.second) {
        askEntity[EntityEvolved](node2, entityId, 8.seconds)(replyTo => Evolve(env, delta, replyTo, tick))
      }

      replayed shouldBe Some(firstEvolved)
    }

  "The keep-majority Split Brain Resolver" should
    "self-down (and CoordinatedShutdown) the minority side when 2 of 3 nodes crash almost simultaneously" in {
      assume(isRedisReachable(), "Skipping: Redis not reachable on localhost:6379")
      assume(isKafkaReachable(), "Skipping: Kafka not reachable on localhost:9092")

      val node1 = startNode(Node1Port, Node1Port)
      val node2 = startNode(Node2Port, Node1Port)
      val node3 = startNode(Node3Port, Node1Port)
      val nodes = Seq(node1, node2, node3)
      waitForCondition(30.seconds)(allUp(nodes, 3)) shouldBe true

      // Crash 2 of the 3 nodes almost simultaneously: from node1's perspective this is
      // indistinguishable from a network partition isolating it as the minority side (1 of 3).
      node2.system.terminate()
      node3.system.terminate()

      noException should be thrownBy
        Await.result(node1.system.whenTerminated, SbrStableAfter + 45.seconds)
    }
}
