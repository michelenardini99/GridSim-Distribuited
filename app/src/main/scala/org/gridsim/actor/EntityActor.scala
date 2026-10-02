package org.gridsim.actor

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, RetentionCriteria}
import org.gridsim.actor.protocol.EntityProtocol.*
import org.gridsim.actor.telemetry.{EntityTelemetryPublisher, TelemetryPublisher}
import org.gridsim.core.behaviour.{EntityEvolutionHandler, EvolutionRequest}
import org.gridsim.core.common.{Energy, Flow}
import org.gridsim.core.model.{Environment, GridEntity, GridEntityState}

import scala.concurrent.duration.{DurationInt, FiniteDuration}

object EntityActor:

  val TypeKey: EntityTypeKey[EntityCommand] = EntityTypeKey("EntityActor")

  def entityId(simulationId: String, localId: String): String = s"$simulationId-$localId"

  final case class State(config: Option[GridEntity], dynamic: Option[GridEntityState])

  def apply(
    persistenceId: PersistenceId,
    handlerFor: GridEntity => EntityEvolutionHandler,
    publisher: EntityTelemetryPublisher = TelemetryPublisher.NoOpEntityTelemetryPublisher
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
          case (State(Some(entity), Some(dyn)), Evolve(env, delta, replyTo, tick)) =>
            val (newState, flow) = handlerFor(entity).evolve(EvolutionRequest(entity, dyn, env, delta))
            Effect
              .persist(Evolved(newState, flow))
              .thenRun { _ =>
                publisher.publish(entity.id, tick, newState, flow)
                replyTo ! EntityEvolved(entity.id, flow)
              }
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
