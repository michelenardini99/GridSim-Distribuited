package org.gridsim.gui.app

import org.gridsim.gui.app.AppEvent._
import org.gridsim.gui.app.Route._
import org.gridsim.gui.model.RunningSimulation
import scalafx.scene.Parent
import scalafx.scene.layout.BorderPane

/** Represents the current navigational route/screen in the application. */
enum Route:
  /** The list of active simulations from the backend API. */
  case RunningSimulations
  /** The scenario list selection view (to create a new simulation). */
  case ScenarioSelection
  /** The active running simulation dashboard view. */
  case Simulation(id: String, preset: String)

/**
 * Representation of the application navigation state.
 *
 * @param route the active route/screen
 */
case class AppState(
  route: Route
)

/** Global application events that trigger navigational route changes. */
enum AppEvent:
  /** Emitted when the user wants to start a new simulation. */
  case StartNewSimulationClicked
  /** Emitted when the user selects an existing simulation to watch. */
  case SimulationSelected(id: String, preset: String)
  /** Emitted when a new simulation is successfully created (scenario loaded remotely). */
  case SimulationCreated
  /** Emitted when the user exits the simulation or scenario selection and wants to go back to the running simulations list. */
  case NavigationBack

/**
 * Coordinates screen routing and dispatching navigational events to transition states.
 *
 * @param render function that accepts the current route and an event dispatch callback, returning the root UI component
 */
class AppRouter(
  render: (Route, AppEvent => Unit) => Parent
):
  private var state = AppState(route = RunningSimulations)

  private val rootPane = new BorderPane:
    center = render(state.route, dispatch)

  /** The root parent component of the routing layout. */
  def root: Parent =
    rootPane

  /** Stops the active simulation if one is currently running locally (Now deprecated in remote mode). */
  def stopActiveSimulation(): Unit = ()

  /**
   * Dispatches a navigation event, transitioning app state and rendering the new route.
   *
   * @param event the navigation event triggering the state transition
   */
  def dispatch(event: AppEvent): Unit =
    event match
      case StartNewSimulationClicked =>
        state = state.copy(route = ScenarioSelection)
        rootPane.center = render(state.route, dispatch)
      case SimulationSelected(id, preset) =>
        state = state.copy(route = Simulation(id, preset))
        rootPane.center = render(state.route, dispatch)
      case SimulationCreated | NavigationBack =>
        state = state.copy(route = RunningSimulations)
        rootPane.center = render(state.route, dispatch)
