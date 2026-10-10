package org.gridsim.gui.viewmodel

import scalafx.beans.property.{BooleanProperty, ObjectProperty, StringProperty}
import org.gridsim.core.simulation.{SimulationControllerState, SimulationSpeed}
import org.gridsim.core.simulation.SimulationControllerState.{PAUSED, RUNNING}
import org.gridsim.gui.model.{ControlState, RunningSimulation, TickDurationUnit}

import scala.concurrent.duration.*

/**
 * ViewModel responsible for managing simulation lifecycle commands and UI state properties.
 * Exposes observable properties to bind with control buttons.
 *
 * @param running the active simulation model and controller wrapper
 * @param onExitCallback a callback to execute when exiting the simulation
 */
class SimulationControlViewModel(
  running: RunningSimulation,
  onExitCallback: () => Unit,
  onTickChanged: () => Unit = () => ()
):
  private val stoppedProperty = BooleanProperty(false)

  /** Text showing the action for the Play/Pause button ("Play" or "Pause"). */
  val playPauseText: StringProperty = StringProperty("Play")

  /** Text representing the current state of the simulation controller (e.g., "RUNNING", "PAUSED", "STOPPED"). */
  val statusText: StringProperty = StringProperty("PAUSED")

  /** True while applying a change received from the backend, so it is not sent back to it. */
  private var syncingFromRemote = false

  private def toAmountAndUnit(duration: FiniteDuration): (Int, TickDurationUnit) =
    val seconds = duration.toSeconds
    if seconds > 0 && seconds % (24 * 3600) == 0 then
      ((seconds / (24 * 3600)).toInt, TickDurationUnit.Days)
    else if seconds > 0 && seconds % 3600 == 0 then
      ((seconds / 3600).toInt, TickDurationUnit.Hours)
    else if seconds > 0 && seconds % 60 == 0 then
      ((seconds / 60).toInt, TickDurationUnit.Minutes)
    else
      (seconds.toInt, TickDurationUnit.Seconds)

  private val (initialAmount, initialUnit) = toAmountAndUnit(running.controller.configuration.delta)

  /** Property bound to the input field specifying the numeric tick amount. */
  val tickAmountText = StringProperty(initialAmount.toString)

  /** Property bound to the combo box specifying the tick's time unit. */
  val tickUnit: ObjectProperty[TickDurationUnit] = ObjectProperty(initialUnit)

  /** Execution speed selected for the simulation lifecycle scheduler. */
  val selectedSpeed: ObjectProperty[SimulationSpeed] =
    ObjectProperty(running.controller.configuration.speed)

  /** Prevents changing lifecycle speed after the controller has stopped. */
  val speedSelectionDisabled: BooleanProperty = BooleanProperty(false)

  /** Indicates whether the Play/Pause button should be disabled. */
  val playPauseDisabled: BooleanProperty = BooleanProperty(false)

  /** Indicates whether the Step Once button should be disabled. */
  val stepDisabled: BooleanProperty = BooleanProperty(false)

  /** Indicates whether the Stop button should be disabled. */
  val stopDisabled: BooleanProperty = BooleanProperty(false)

  /** Indicates whether the Exit button should be disabled. */
  val exitDisabled: BooleanProperty = BooleanProperty(false)

  def parsed: Either[String, FiniteDuration] =
    for
      tickAmount <- tickAmountText.value.trim.toIntOption
        .toRight("Not a valid tick amount inserted")
        .filterOrElse(_ > 0, "Tick amount must be greater than zero")
    yield tickUnit.value.toDuration(tickAmount)

  private def updateTick(): Unit =
    if !syncingFromRemote then parsed match
      case Left(err) => ()
      case Right(tickDelta) =>
        running.controller.setTick(tickDelta)
        onTickChanged()

  tickAmountText.onChange { (_, _, _) => updateTick() }
  tickUnit.onChange { (_, _, _) => updateTick() }

  val detailsLayout = ObjectProperty(SimulationViewLayout.Tabs)

  def toggleLayout(): Unit =
    detailsLayout.value = detailsLayout.value match
      case SimulationViewLayout.Tabs => SimulationViewLayout.Split
      case _ => SimulationViewLayout.Tabs

  /**
   * Updates the states of the bindable properties based on the simulation controller's state.
   *
   * @param controllerState the current status of the simulation execution
   */
  def update(controllerState: SimulationControllerState): Unit =
    val isStopped = stoppedProperty.value
    playPauseDisabled.value = isStopped
    stopDisabled.value = isStopped
    speedSelectionDisabled.value = isStopped
    exitDisabled.value = false

    if isStopped then
      stepDisabled.value = true
      playPauseText.value = "Play"
      statusText.value = "STOPPED"
    else
      stepDisabled.value = controllerState == RUNNING
      playPauseText.value = if controllerState == RUNNING then "Pause" else "Play"
      statusText.value = controllerState.toString

  /**
   * Aligns the controls with a status/configuration change made by any client.
   * Must be called on the JavaFX Application Thread.
   */
  def syncWith(control: ControlState): Unit =
    if control.stopped then stoppedProperty.value = true
    syncingFromRemote = true
    try
      selectedSpeed.value = control.conf.speed
      if parsed.toOption.forall(_ != control.conf.delta) then
        val (amount, unit) = toAmountAndUnit(control.conf.delta)
        tickUnit.value = unit
        tickAmountText.value = amount.toString
    finally syncingFromRemote = false
    update(control.status)
    onTickChanged()

  /** Toggles the simulation status between running and paused. */
  def togglePlayPause(): Unit =
    if !stoppedProperty.value then
      running.controller.simulationControllerState match
        case RUNNING => running.controller.pause()
        case PAUSED  => running.controller.resume()
    update(running.controller.simulationControllerState)

  /** Advances the simulation by a single tick if currently paused. */
  def stepOnce(): Unit =
    if !stoppedProperty.value && running.controller.simulationControllerState == PAUSED then
      running.controller.stepOnce()

  /** Changes the delay used when the controller schedules its next tick. */
  def selectSpeed(speed: SimulationSpeed): Unit =
    if !stoppedProperty.value then
      running.controller.setSpeed(speed)
      selectedSpeed.value = speed

  /** Exits the simulation view without stopping the backend simulation. */
  def exit(): Unit =
    onExitCallback()
