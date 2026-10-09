package org.gridsim.gui.app

import org.gridsim.gui.ports.{
  RemoteScenarioPresetLoader,
  DslScenarioPresetRepository,
  HttpSimulationApiClient
}
import scalafx.application.JFXApp3
import scalafx.scene.Scene

import org.gridsim.gui.model.ClientConfig

/** Main application entry point for the GridSim JavaFX Graphical User
  * Interface.
  *
  * Configures the primary stage, instantiates the repository, loader, router,
  * and wires global CSS stylesheets.
  */
object GuiApp extends JFXApp3:
  /** Initializes the UI routing engine and configures the main window/stage
    * dimensions.
    */
  override def start(): Unit =
    // Parse CLI args
    val namedArgs = parameters.named
    val apiEndpoint = namedArgs.getOrElse("api", "http://localhost:8080")
    val kafkaServers = namedArgs.getOrElse("kafka", "localhost:9092")
    val config = ClientConfig(apiEndpoint, kafkaServers)
    
    import scala.concurrent.ExecutionContext.Implicits.global
    val apiClient = new HttpSimulationApiClient(config)

    val renderer = new SceneBuilder(
      apiClient = apiClient,
      config = config,
      scenarioRepo = new DslScenarioPresetRepository,
      scenarioLoader = new RemoteScenarioPresetLoader(apiClient)
    )
    val router = new AppRouter(
      render = renderer.render
    )

    stage = new JFXApp3.PrimaryStage:
      title = "GridSim"
      scene = new Scene(1300, 700):
        stylesheets.add(getClass.getResource("/gui/gridsim.css").toExternalForm)
        root = router.root
      onCloseRequest = _ => router.stopActiveSimulation()
