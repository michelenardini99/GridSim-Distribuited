package org.gridsim.gui.ports

import org.gridsim.gui.model.ScenarioRunConfig

class RemoteScenarioPresetLoader(apiClient: SimulationApiClient) extends ScenarioPresetLoader[String] {
  
  import scala.concurrent.Await
  import scala.concurrent.duration._

  override def load(runConfig: ScenarioRunConfig): Either[String, String] = {
    // We send the preset name via HTTP POST using the API client.
    try {
      val futureId = apiClient.createSimulation(runConfig.presetId.value)
      // Blocking wait since the loader interface is synchronous and called on UI event.
      val id = Await.result(futureId, 5.seconds)
      Right(id)
    } catch {
      case e: Exception => Left(s"Failed to start simulation: ${e.getMessage}")
    }
  }
}
