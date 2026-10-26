package org.gridsim.actor

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import org.gridsim.actor.protocol.Serializable

import scala.concurrent.duration.DurationInt

object SimulationRegistryActor:

  sealed trait Command extends Serializable
  final case class RegisterSimulation(id: String, preset: String, replyTo: ActorRef[Registered]) extends Command
  final case class UnregisterSimulation(id: String, replyTo: ActorRef[Unregistered]) extends Command
  final case class GetSimulations(replyTo: ActorRef[SimulationsList]) extends Command

  final case class Registered(id: String) extends Serializable
  final case class Unregistered(id: String) extends Serializable
  final case class SimulationInfo(id: String, preset: String) extends Serializable
  final case class SimulationsList(simulations: List[SimulationInfo]) extends Serializable

  sealed trait Event extends Serializable
  final case class SimulationRegistered(id: String, preset: String) extends Event
  final case class SimulationUnregistered(id: String) extends Event

  final case class State(simulations: Map[String, SimulationInfo] = Map.empty) extends Serializable

  def apply(simulations: Map[String, SimulationInfo] = Map.empty): Behavior[Command] =
    Behaviors.receiveMessage {
      case RegisterSimulation(id, preset, replyTo) =>
        replyTo ! Registered(id)
        apply(simulations + (id -> SimulationInfo(id, preset)))

      case UnregisterSimulation(id, replyTo) =>
        replyTo ! Unregistered(id)
        apply(simulations - id)

      case GetSimulations(replyTo) =>
        replyTo ! SimulationsList(simulations.values.toList)
        Behaviors.same
    }

  /** Event-sourced behavior backed by Pekko Persistence (e.g. Redis journal in cluster mode),
    * ensuring that the active simulations registry survives master failover and scaling.
    */
  def persistent(
      persistenceId: PersistenceId = PersistenceId.ofUniqueId("SimulationRegistry")
  ): Behavior[Command] =
    EventSourcedBehavior[Command, Event, State](
      persistenceId = persistenceId,
      emptyState = State(),
      commandHandler = (state, cmd) =>
        cmd match
          case RegisterSimulation(id, preset, replyTo) =>
            Effect
              .persist(SimulationRegistered(id, preset))
              .thenRun(_ => replyTo ! Registered(id))

          case UnregisterSimulation(id, replyTo) =>
            Effect
              .persist(SimulationUnregistered(id))
              .thenRun(_ => replyTo ! Unregistered(id))

          case GetSimulations(replyTo) =>
            replyTo ! SimulationsList(state.simulations.values.toList)
            Effect.none
      ,
      eventHandler = (state, event) =>
        event match
          case SimulationRegistered(id, preset) =>
            state.copy(simulations = state.simulations + (id -> SimulationInfo(id, preset)))
          case SimulationUnregistered(id) =>
            state.copy(simulations = state.simulations - id)
    ).onPersistFailure(
      SupervisorStrategy.restartWithBackoff(
        minBackoff = 200.millis,
        maxBackoff = 5.seconds,
        randomFactor = 0.2
      )
    )
