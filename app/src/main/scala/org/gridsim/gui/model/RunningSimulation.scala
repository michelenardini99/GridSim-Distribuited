package org.gridsim.gui.model

import cats.effect.IO
import fs2.concurrent.SignallingRef
import org.gridsim.core.observability.SimulationData
import org.gridsim.core.simulation.{SimulationConf, SimulationController, SimulationControllerState, SimulationModel}
import org.gridsim.statistics.StatisticsRegistry

/**
 * Lifecycle status and configuration of a simulation as last reported by the backend.
 *
 * @param stopped whether the simulation has been stopped for good (by any client)
 */
case class ControlState(status: SimulationControllerState, conf: SimulationConf, stopped: Boolean = false)

/**
 * Representation of an active simulation loop setup.
 *
 * Combines the domain model topology, the execution controller, and the stream of updates.
 *
 * @param model the static topology and parameters configuration of the grid
 * @param controller the engine state controller (handling start, pause, resume, step)
 * @param snapshotSignal signaling stream emitting simulation snapshot updates
 * @param controlSignal signaling stream emitting control changes made by any client (remote simulations only)
 */
case class RunningSimulation(
  name: String,
  model: SimulationModel,
  controller: SimulationController,
  snapshotSignal: SignallingRef[IO, SimulationData.SimulationSnapshot],
  statisticsSignal: SignallingRef[IO, StatisticsRegistry.engine.State],
  controlSignal: Option[SignallingRef[IO, ControlState]] = None
)
