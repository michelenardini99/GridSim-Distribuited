package org.gridsim.actor

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.gridsim.actor.protocol.Serializable

object SimulationRegistryActor:

  sealed trait Command extends Serializable
  final case class RegisterSimulation(id: String, preset: String, replyTo: ActorRef[Registered]) extends Command
  final case class UnregisterSimulation(id: String, replyTo: ActorRef[Unregistered]) extends Command
  final case class GetSimulations(replyTo: ActorRef[SimulationsList]) extends Command

  final case class Registered(id: String) extends Serializable
  final case class Unregistered(id: String) extends Serializable
  final case class SimulationInfo(id: String, preset: String) extends Serializable
  final case class SimulationsList(simulations: List[SimulationInfo]) extends Serializable

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
