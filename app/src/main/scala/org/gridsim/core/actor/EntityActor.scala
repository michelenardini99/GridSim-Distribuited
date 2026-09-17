package org.gridsim.core.actor

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.{EventSourcedBehavior, RetentionCriteria}
import org.apache.pekko.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import org.gridsim.core.behaviour.{EntityEvolutionHandler, EvolutionRequest}
import org.gridsim.core.common.{Energy, Flow}
import org.gridsim.core.model.{Environment, GridEntity, GridEntityState}

import scala.concurrent.duration.{DurationInt, FiniteDuration}

object EntityActor:

  val TypeKey: EntityTypeKey[EntityCommand] = EntityTypeKey("EntityActor")

  def entityId(simulationId: String, localId: String): String = s"$simulationId-$localId"

  sealed trait EntityCommand

  final case class Initialize(
    entity: GridEntity,
    initialState: GridEntityState,
    replyTo: ActorRef[Ack.type]
  ) extends EntityCommand

  final case class Evolve(
    env: Environment,
    delta: FiniteDuration,
    replyTo: ActorRef[EntityEvolved]
  ) extends EntityCommand

  final case class Ack() extends EntityCommand

  final case class EntityEvolved(
    id: String,
    state: GridEntityState,
    flow: Flow[Energy]
  )

  sealed trait EntityEvent

  final case class Initialized(
    entity: GridEntity,
    initialState: GridEntityState
  ) extends EntityEvent

  final case class Evolved(
    newState: GridEntityState,
    flow: Flow[Energy]
  ) extends EntityEvent

  final case class State(config: Option[GridEntity], dynamic: Option[GridEntityState])

  def apply(
    persistenceId: PersistenceId,
    handlerFor: GridEntity => EntityEvolutionHandler
  ): Behavior[EntityCommand] =
    EventSourcedBehavior[EntityCommand, EntityEvent, State](
      persistenceId,
      emptyState = State(None, None),
      commandHandler = (state, command) => {
        (state, command) match
          case (State(None, _), Initialize(entity, initialState, replyTo)) =>
            Effect
              .persist(Initialized(entity, initialState))
              .thenRun(_ => replyTo ! Ack)
          case (State(Some(entity), Some(dyn)), Evolve(env, delta, replyTo)) =>
            val (newState, flow) = handlerFor(entity).evolve(EvolutionRequest(entity, dyn, env, delta))
            Effect
              .persist(Evolved(newState, flow))
              .thenRun(_ => replyTo ! EntityEvolved(entity.id, newState, flow))
          case _ => Effect.unhandled
      },
      eventHandler = (state, event) => {
       event match
          case Initialized(entity, initialState) => State(Some(entity), Some(initialState))
          case Evolved(newState, _) => state.copy(dynamic = Some(newState))
      }
    )
    .withRetention(
      RetentionCriteria
        .snapshotEvery(numberOfEvents = 100, keepNSnapshots = 2)
        .withDeleteEventsOnSnapshot
    )
    .onPersistFailure(
      SupervisorStrategy.restartWithBackoff(
        minBackoff = 200.millis, maxBackoff = 5.seconds, randomFactor = 0.2
      )
    )


