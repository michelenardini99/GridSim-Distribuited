package org.gridsim.integration.gui

import org.gridsim.core.common.*
import org.gridsim.core.model.*
import org.gridsim.core.model.network.{Cable, CableConnections, ExternalGrid, GridGraph}
import org.gridsim.core.simulation.{SimulationModel, SimulationSpeed}
import org.gridsim.core.simulation.SimulationControllerState.{PAUSED, RUNNING}
import org.gridsim.gui.model.ClientConfig
import org.gridsim.gui.ports.{HttpSimulationApiClient, RemoteSimulationBuilder}
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import java.net.{InetSocketAddress, Socket}
import java.net.http.HttpConnectTimeoutException
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.*

@RunWith(classOf[JUnitRunner])
class HttpApiUnavailableIntegrationSpec extends AnyFlatSpecLike with Matchers {

  private val BootstrapServers = "localhost:9092"
  private val UnreachableEndpoint = "http://192.0.2.1:8080"

  private def isKafkaReachable(): Boolean =
    try
      val socket = new Socket()
      socket.connect(new InetSocketAddress("127.0.0.1", 9092), 1000)
      socket.close()
      true
    catch
      case _: Exception => false

  private case class DummyEntity(id: String) extends GridEntity

  private def minimalModel(): SimulationModel =
    val cable = Cable(CableConnections("solar-1", "eg"), maxCapacity = 100.kw)
    val gridGraph = GridGraph(nodes = List(DummyEntity("solar-1"), ExternalGrid("eg")), cables = List(cable))
    SimulationModel(gridGraph)

  "HttpSimulationApiClient" should "fail within roughly its connectTimeout(5s), not hang indefinitely, against an unresponsive host" in {
    implicit val ec: ExecutionContext = ExecutionContext.global
    val client = new HttpSimulationApiClient(ClientConfig(UnreachableEndpoint, BootstrapServers))

    val start = System.nanoTime()
    a[HttpConnectTimeoutException] should be thrownBy
      Await.result(client.getSimulationStatus("whatever"), 20.seconds)
    val elapsedMs = (System.nanoTime() - start) / 1000000L
    
    elapsedMs should be < 8000L
  }

  "RemoteSimulationBuilder.build" should "fall back to PAUSED, without hanging or throwing, when the backend HTTP API is unreachable" in {
    assume(isKafkaReachable(), s"Skipping test: Kafka broker not reachable on $BootstrapServers")
    implicit val ec: ExecutionContext = ExecutionContext.global

    val config = ClientConfig(UnreachableEndpoint, BootstrapServers)
    val apiClient = new HttpSimulationApiClient(config)

    val start = System.nanoTime()
    val runningSim = RemoteSimulationBuilder.build("test-sim-http-down", "test-preset", minimalModel(), apiClient, config)
    val elapsedMs = (System.nanoTime() - start) / 1000000L

    runningSim.controller.simulationControllerState shouldBe PAUSED
    elapsedMs should be < 8000L
  }

  it should "optimistically flip the local state to RUNNING on start(), even though the backend call never succeeds" in {
    assume(isKafkaReachable(), s"Skipping test: Kafka broker not reachable on $BootstrapServers")
    implicit val ec: ExecutionContext = ExecutionContext.global

    val config = ClientConfig(UnreachableEndpoint, BootstrapServers)
    val apiClient = new HttpSimulationApiClient(config)
    val runningSim = RemoteSimulationBuilder.build("test-sim-http-down-start", "test-preset", minimalModel(), apiClient, config)

    runningSim.controller.simulationControllerState shouldBe PAUSED

    // start() fires the HTTP call and discards the returned Future: it neither blocks on nor
    // observes the eventual connect-timeout failure, so no exception ever surfaces here.
    noException should be thrownBy runningSim.controller.start()
    runningSim.controller.simulationControllerState shouldBe RUNNING
  }
}
