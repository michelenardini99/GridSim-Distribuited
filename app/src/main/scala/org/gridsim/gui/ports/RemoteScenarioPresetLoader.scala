package org.gridsim.gui.ports

import org.gridsim.gui.model.{ScenarioRunConfig, ClientConfig}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI
import java.time.Duration
import spray.json._

case class CreateSimulationRequest(preset: String)
case class SimulationResponse(id: String, preset: String)

object RemoteScenarioJsonProtocol extends DefaultJsonProtocol {
  implicit val createFormat: RootJsonFormat[CreateSimulationRequest] = jsonFormat1(CreateSimulationRequest.apply)
  implicit val responseFormat: RootJsonFormat[SimulationResponse] = jsonFormat2(SimulationResponse.apply)
}

import RemoteScenarioJsonProtocol._

class RemoteScenarioPresetLoader(config: ClientConfig) extends ScenarioPresetLoader[String] {
  
  private val client = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(5))
    .build()

  override def load(runConfig: ScenarioRunConfig): Either[String, String] = {
    // We send the preset name via HTTP POST. 
    // Currently the API only takes the preset name, but in the future we could send tickDelta and startDate too.
    val requestBody = CreateSimulationRequest(runConfig.presetId.value).toJson.compactPrint
    
    val req = HttpRequest.newBuilder()
      .uri(URI.create(s"${config.apiEndpoint}/api/simulations"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(requestBody))
      .build()

    try {
      val response = client.send(req, HttpResponse.BodyHandlers.ofString())
      if (response.statusCode() == 200 || response.statusCode() == 201) {
        val respObj = response.body().parseJson.convertTo[SimulationResponse]
        Right(respObj.id)
      } else {
        Left(s"API returned status ${response.statusCode()}: ${response.body()}")
      }
    } catch {
      case e: Exception => Left(s"Failed to connect to API: ${e.getMessage}")
    }
  }
}
