package org.gridsim.gui.app

import org.gridsim.gui.app.AppEvent._
import org.gridsim.gui.app.Route._
import org.gridsim.gui.viewmodel.{
  ScenarioSelectionViewModel,
  SimulationCoordinator
}
import org.gridsim.gui.model.RunningSimulation
import org.gridsim.gui.ports.{ScenarioPresetLoader, ScenarioPresetRepository, SimulationApiClient}
import org.gridsim.gui.view.{ScenarioSelectionView, SimulationView, RunningSimulationsView}
import scalafx.scene.Parent
import org.gridsim.gui.viewmodel.SimulationViewLayout

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
          onSimulationSelected = id => dispatch(SimulationSelected(id))
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
      case Simulation(simId) =>
        // Temporarily handling string ID, we'll need to rewrite SimulationCoordinator for remote
        // For now just passing a dummy or we need to refactor Simulation Coordinator later
        // as the user requested only the two windows for now.
        new scalafx.scene.layout.VBox {
           children = Seq(new scalafx.scene.control.Label(s"Watching simulation: $simId"))
        }

