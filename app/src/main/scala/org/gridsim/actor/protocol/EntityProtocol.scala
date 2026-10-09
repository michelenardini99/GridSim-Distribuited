package org.gridsim.actor.protocol

import org.apache.pekko.actor.typed.ActorRef
import org.gridsim.core.common.{Energy, Flow}
import org.gridsim.core.model.{Environment, GridEntity, GridEntityState}

import scala.concurrent.duration.FiniteDuration

object EntityProtocol:
  sealed trait EntityCommand extends Serializable

  final case class Initialize(
    entity: GridEntity,
    initialState: Option[GridEntityState],
    replyTo: ActorRef[Ack.type]
  ) extends EntityCommand

  final case class Evolve(
    env: Environment,
    delta: FiniteDuration,
    replyTo: ActorRef[EntityEvolved],
    tick: Long = 0L
  ) extends EntityCommand

  case object Ack extends EntityCommand

  final case class EntityEvolved(
    id: String,
    flow: Flow[Energy]
  )

  sealed trait EntityEvent extends Serializable

  final case class Initialized(
    entity: GridEntity,
    initialState: Option[GridEntityState]
  ) extends EntityEvent

  final case class Evolved(
    newState: GridEntityState,
    flow: Flow[Energy]
  ) extends EntityEvent
