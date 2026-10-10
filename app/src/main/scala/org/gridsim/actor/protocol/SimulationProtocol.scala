package org.gridsim.actor.protocol

import org.apache.pekko.actor.typed.ActorRef
import org.gridsim.actor.protocol.EntityProtocol.EntityEvolved
import org.gridsim.core.model.Environment
import org.gridsim.core.simulation.{SimulationConf, SimulationModel, SimulationSpeed, SimulationState}

import scala.concurrent.duration.FiniteDuration

object SimulationProtocol:

  sealed trait SimulationCommand extends Serializable

  final case class Initialize(
    model: SimulationModel,
    initialState: SimulationState,
    conf: SimulationConf,
    replyTo: ActorRef[Ack.type]
  ) extends SimulationCommand

  final case class EntitiesInitialized(replyTo: ActorRef[Ack.type]) extends SimulationCommand
  final case class EntitiesInitializationFailed(ex: Throwable, replyTo: ActorRef[Ack.type]) extends SimulationCommand
  final case class GetStatus(replyTo: ActorRef[String]) extends SimulationCommand

  case object Start extends SimulationCommand
  case object Pause extends SimulationCommand
  case object Stop extends SimulationCommand
  case object Step extends SimulationCommand

  final case class UpdateSpeed(speed: SimulationSpeed) extends SimulationCommand
  final case class UpdateTickDelta(delta: FiniteDuration) extends SimulationCommand

  case object Ack extends SimulationCommand
  case object TickTimer extends SimulationCommand

  final case class EntitiesEvolved(
    newEnv: Environment,
    results: Iterable[EntityEvolved],
    scheduleNext: Boolean
  ) extends SimulationCommand

  final case class TickFailed(cause: Throwable) extends SimulationCommand

  sealed trait SimulationEvent extends Serializable

  final case class Initialized(
    model: SimulationModel,
    environment: Environment,
    conf: SimulationConf
  ) extends SimulationEvent

  case object Started extends SimulationEvent
  case object Paused extends SimulationEvent
  case object Stopped extends SimulationEvent

  final case class TickAdvanced(env: Environment, tick: Long = 0L) extends SimulationEvent

  final case class SpeedUpdated(speed: SimulationSpeed) extends SimulationEvent
  final case class TickDeltaUpdated(delta: FiniteDuration) extends SimulationEvent
