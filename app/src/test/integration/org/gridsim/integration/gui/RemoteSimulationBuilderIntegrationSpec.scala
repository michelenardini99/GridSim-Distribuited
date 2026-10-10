package org.gridsim.integration.gui

import cats.effect.unsafe.implicits.global
import org.apache.kafka.clients.producer.ProducerRecord
import org.gridsim.core.common.*
import org.gridsim.core.common.Energy.*
import org.gridsim.core.common.Flow.*
import org.gridsim.core.common.Power.*
import org.gridsim.core.model.*
import org.gridsim.core.model.network.{Cable, CableConnections, ExternalGrid, GridGraph}
import org.gridsim.core.observability.serialization.SimulationDataCodecs
import org.gridsim.core.simulation.{SimulationModel, SimulationSpeed}
import org.gridsim.gui.model.ClientConfig
import org.gridsim.gui.ports.{RemoteSimulationBuilder, SimulationApiClient, SimulationItem}
import org.gridsim.actor.telemetry.TelemetryPublisher
import org.junit.runner.RunWith
import scala.concurrent.duration.FiniteDuration
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import java.net.{InetSocketAddress, Socket}
import java.time.LocalDateTime
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.*

/**
 * End-to-end integration test validating that RemoteSimulationBuilder can connect
 * to Kafka, decode incoming telemetry correctly, and emit valid state to its SignallingRefs.
 */
@RunWith(classOf[JUnitRunner])
class RemoteSimulationBuilderIntegrationSpec
    extends AnyFlatSpecLike
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

  private case class DummyEntity(id: String) extends GridEntity

  "RemoteSimulationBuilder" should "consume Kafka telemetry and update RunningSimulation state" in {
    assume(isKafkaReachable(), s"Skipping test: Kafka broker not reachable on $BootstrapServers")

    val simId = s"test-sim-${UUID.randomUUID()}"
    val env = Environment(LocalDateTime.of(2026, 10, 2, 12, 0, 0), 0.seconds)
    val delta = 1.minute
    val cable = Cable(CableConnections("solar-1", "eg"), maxCapacity = 100.kw)
    val gridGraph = GridGraph(nodes = List(DummyEntity("solar-1"), ExternalGrid("eg")), cables = List(cable))
    val model = SimulationModel(gridGraph)
    val config = ClientConfig("http://localhost:8080", BootstrapServers)

    val mockApiClient = new SimulationApiClient {
      override def getSimulations(): Future[List[SimulationItem]] = Future.successful(Nil)
      override def createSimulation(preset: String): Future[String] = Future.successful(simId)
      override def startSimulation(id: String): Future[Unit] = Future.unit
      override def pauseSimulation(id: String): Future[Unit] = Future.unit
      override def stopSimulation(id: String): Future[Unit] = Future.unit
      override def stepSimulation(id: String): Future[Unit] = Future.unit
      override def getSimulationStatus(id: String): Future[String] = Future.successful("RunningStatus")
      override def setSimulationSpeed(id: String, speed: SimulationSpeed): Future[Unit] = Future.unit
      override def setSimulationTickDuration(id: String, delta: FiniteDuration): Future[Unit] = Future.unit
    }

    // 1. Initialize RemoteSimulationBuilder
    val runningSim = RemoteSimulationBuilder.build(simId, "test-preset", model, mockApiClient, config)

    // Initially should be empty snapshot
    val initialSnapshot = runningSim.snapshotSignal.get.unsafeRunSync()
    initialSnapshot.entityStates shouldBe empty

    // 2. Publish Entity Telemetry
    val entityTopic = s"grid.entities.$simId"
    val tickTopic = s"grid.ticks.$simId"
    
    val solarState = SolarPanelState("solar-1", efficiency = 0.20)
    val flowResult = Surplus(Energy(12.5))
    val entityBytes = SimulationDataCodecs.telemetryToBinary("solar-1", 0L, solarState, flowResult)
    producer.send(new ProducerRecord[String, Array[Byte]](entityTopic, "solar-1", entityBytes)).get()

    // 3. Publish Tick Telemetry
    val newEnv = env.advance(delta)
    val cableLoads = Map(cable -> Energy(12.5))
    val tickBytes = SimulationDataCodecs.gridTickToBinary(0L, newEnv, cableLoads, delta)
    producer.send(new ProducerRecord[String, Array[Byte]](tickTopic, "0", tickBytes)).get() // block until sent

    // 4. Wait for RemoteSimulationBuilder's Kafka consumer to poll and update
    var updated = false
    val deadline = System.currentTimeMillis() + 5000 // 5 seconds wait max
    while (!updated && System.currentTimeMillis() < deadline) {
      val snap = runningSim.snapshotSignal.get.unsafeRunSync()
      if (snap.entityStates.nonEmpty) {
        updated = true
      } else {
        Thread.sleep(100)
      }
    }

    updated shouldBe true

    val finalSnap = runningSim.snapshotSignal.get.unsafeRunSync()
    finalSnap.environment shouldBe newEnv
    finalSnap.entityStates("solar-1") shouldBe solarState
    finalSnap.entityFlows("solar-1") shouldBe flowResult
    finalSnap.cableLoads(cable) shouldBe Energy(12.5)
    finalSnap.delta shouldBe delta

    val stats = runningSim.statisticsSignal.get.unsafeRunSync()
  }
}
