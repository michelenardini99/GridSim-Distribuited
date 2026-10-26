package org.gridsim.core.simulation

import org.gridsim.core.model.network.GridGraph

/**
 * Immutable configuration of a simulation.
 *
 * The model contains only data that remain constant while the simulation is
 * running. Runtime values such as entity states and cable loads belong to
 * [[SimulationState]].
 *
 * @param grid the static entities and cable topology of the simulated micro-grid
 */
final case class SimulationModel(grid: GridGraph) extends org.gridsim.actor.protocol.Serializable
