package org.gridsim.gui.ports

import org.gridsim.core.simulation.SimulationSpeed
import org.gridsim.gui.model.ClientConfig
import spray.json.*

import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI
import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

case class SimulationItem(id: String, preset: String)
case class SimulationListResponse(simulations: List[SimulationItem])
case class CreateSimulationRequest(preset: String)
case class SimulationResponse(id: String, preset: String)
case class MessageResponse(message: String)

object ApiJsonProtocol extends DefaultJsonProtocol {
  implicit val simulationItemFormat: RootJsonFormat[SimulationItem] = jsonFormat2(SimulationItem.apply)
  implicit val simulationListResponseFormat: RootJsonFormat[SimulationListResponse] = jsonFormat1(SimulationListResponse.apply)
  implicit val createFormat: RootJsonFormat[CreateSimulationRequest] = jsonFormat1(CreateSimulationRequest.apply)
  implicit val responseFormat: RootJsonFormat[SimulationResponse] = jsonFormat2(SimulationResponse.apply)
  implicit val msgResponseFormat: RootJsonFormat[MessageResponse] = jsonFormat1(MessageResponse.apply)
}

/**
 * Port representing the backend API operations.
 */
trait SimulationApiClient:
  def getSimulations(): Future[List[SimulationItem]]
  def createSimulation(preset: String): Future[String]
  def startSimulation(id: String): Future[Unit]
  def pauseSimulation(id: String): Future[Unit]
  def stopSimulation(id: String): Future[Unit]
  def stepSimulation(id: String): Future[Unit]

  def getSimulationStatus(id: String): Future[String]
  def setSimulationSpeed(id: String, speed: SimulationSpeed): Future[Unit]
  def setSimulationTickDuration(id: String, delta: FiniteDuration): Future[Unit]

/**
 * HTTP implementation of the SimulationApiClient using java.net.http
 */
class HttpSimulationApiClient(config: ClientConfig)(implicit ec: ExecutionContext) extends SimulationApiClient:
  import ApiJsonProtocol._

  private val client = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(5))
    .build()

  override def getSimulations(): Future[List[SimulationItem]] = Future {
    val req = HttpRequest.newBuilder()
      .uri(URI.create(s"${config.apiEndpoint}/api/simulations"))
      .GET()
      .build()

    val response = client.send(req, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() == 200) {
      response.body().parseJson.convertTo[SimulationListResponse].simulations
    } else {
      throw new RuntimeException(s"Failed to fetch: HTTP ${response.statusCode()}")
    }
  }

  override def createSimulation(preset: String): Future[String] = Future {
    val requestBody = CreateSimulationRequest(preset).toJson.compactPrint

    val req = HttpRequest.newBuilder()
      .uri(URI.create(s"${config.apiEndpoint}/api/simulations"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(requestBody))
      .build()

    val response = client.send(req, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() == 200 || response.statusCode() == 201) {
      response.body().parseJson.convertTo[SimulationResponse].id
    } else {
      throw new RuntimeException(s"API returned status ${response.statusCode()}: ${response.body()}")
    }
  }

  override def startSimulation(id: String): Future[Unit] = sendCommand(id, "start")

  override def pauseSimulation(id: String): Future[Unit] = sendCommand(id, "pause")

  override def stepSimulation(id: String): Future[Unit] = sendCommand(id, "step")

  override def getSimulationStatus(id: String): Future[String] = Future {
    val req = HttpRequest.newBuilder()
      .uri(URI.create(s"${config.apiEndpoint}/api/simulations/$id/status"))
      .GET()
      .build()

    val response = client.send(req, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() == 200) {
      response.body().parseJson.convertTo[MessageResponse].message
    } else {
      throw new RuntimeException(s"API returned status ${response.statusCode()}: ${response.body()}")
    }
  }

  override def stopSimulation(id: String): Future[Unit] = Future {
    val req = HttpRequest.newBuilder()
      .uri(URI.create(s"${config.apiEndpoint}/api/simulations/$id"))
      .DELETE()
      .build()

    val response = client.send(req, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() != 200) {
      throw new RuntimeException(s"API returned status ${response.statusCode()}: ${response.body()}")
    }
  }

  private def sendCommand(id: String, command: String): Future[Unit] = Future {
    val req = HttpRequest.newBuilder()
      .uri(URI.create(s"${config.apiEndpoint}/api/simulations/$id/$command"))
      .POST(HttpRequest.BodyPublishers.noBody())
      .build()

    val response = client.send(req, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() != 200) {
      throw new RuntimeException(s"API returned status ${response.statusCode()}: ${response.body()}")
    }
  }

  override def setSimulationSpeed(id: String, speed: SimulationSpeed): Future[Unit] = Future {
    val body = s"""{"speed":"${speed.toString}"}"""
    val req = HttpRequest.newBuilder()
      .uri(URI.create(s"${config.apiEndpoint}/api/simulations/$id/speed"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()
    val response = client.send(req, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() != 200) throw new RuntimeException(s"API returned ${response.statusCode()}: ${response.body()}")
  }

  override def setSimulationTickDuration(id: String, delta: FiniteDuration): Future[Unit] = Future {
    val body = s"""{"deltaSeconds":${delta.toSeconds}}"""
    val req = HttpRequest.newBuilder()
      .uri(URI.create(s"${config.apiEndpoint}/api/simulations/$id/tick"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()
    val response = client.send(req, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() != 200) throw new RuntimeException(s"API returned ${response.statusCode()}: ${response.body()}")
  }
