package org.gridsim.gui.app

import org.gridsim.gui.app.AppEvent._
import org.gridsim.gui.app.Route._
import org.gridsim.gui.viewmodel.{
  ScenarioSelectionViewModel,
  SimulationCoordinator
}
import org.gridsim.gui.model.{RunningSimulation, ClientConfig}
import org.gridsim.gui.ports.{ScenarioPresetLoader, ScenarioPresetRepository, SimulationApiClient}
import org.gridsim.gui.view.{ScenarioSelectionView, SimulationView, RunningSimulationsView}
import scalafx.scene.Parent
import org.gridsim.gui.viewmodel.SimulationViewLayout
import org.gridsim.dsl.scenarios.GridScenarioCatalog
import org.gridsim.gui.ports.RemoteSimulationBuilder
import scala.concurrent.duration.DurationInt

/** Factory class responsible for instantiating the UI View components
  * corresponding to the active route.
  *
  * @param scenarioRepo
  *   the repository containing the available presets
  * @param scenarioLoader
  *   the loader to construct the running simulation context
  */
class SceneBuilder(
    apiClient: SimulationApiClient,
    config: ClientConfig,
    scenarioRepo: ScenarioPresetRepository,
    scenarioLoader: ScenarioPresetLoader[String]
):
  /** Renders the View component matching the current navigation Route.
    *
    * @param route
    *   the current route to render
    * @param dispatch
    *   event dispatching callback used by views to trigger state transitions
    * @return
    *   the rendered parent node for the requested route
    */
  def render(route: Route, dispatch: AppEvent => Unit): Parent =
    route match
      case RunningSimulations =>
        new RunningSimulationsView(
          apiClient = apiClient,
          onNewSimulation = () => dispatch(StartNewSimulationClicked),
          onSimulationSelected = (id, preset) => dispatch(SimulationSelected(id, preset))
        )
      case ScenarioSelection =>
        ScenarioSelectionView(
          viewModel = ScenarioSelectionViewModel(
            scenarioRepo = scenarioRepo,
            loader = scenarioLoader
          ),
          onScenarioLoaded = { _ =>
            dispatch(SimulationCreated)
          }
        )
      case Simulation(simId, preset) =>
        val scenario = GridScenarioCatalog.byId(preset).getOrElse(throw new IllegalArgumentException(s"Unknown preset: $preset"))
        val builder = scenario.build(1.minute)
        val (model, state) = builder.build().fold(errs => throw new IllegalArgumentException(errs.toString), identity)
        val running = RemoteSimulationBuilder.build(simId, preset, model, apiClient, config)
        val coordinator = new SimulationCoordinator(running, () => dispatch(NavigationBack))
        new SimulationView(coordinator)

