package org.gridsim.actor

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior, Scheduler, SupervisorStrategy}
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityRef
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey
import org.apache.pekko.dispatch.Futures
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import org.apache.pekko.persistence.typed.RecoveryCompleted
import org.apache.pekko.util.Timeout
import org.gridsim.actor.SimulationActor.Status.{IdleStatus, PausedStatus, RunningStatus, StoppedStatus}
import org.gridsim.actor.protocol.EntityProtocol.{EntityCommand, EntityEvolved, Evolve}
import org.gridsim.actor.protocol.SimulationProtocol.{Step, Stop, Ack, EntitiesEvolved, Initialize, Initialized, Pause, Paused, SimulationCommand, SimulationEvent, SpeedUpdated, Start, Started, Stopped, TickAdvanced, TickDeltaUpdated, TickFailed, TickTimer, UpdateSpeed, UpdateTickDelta}
import org.gridsim.actor.telemetry.{SimulationControlPublisher, SimulationTickPublisher, TelemetryPublisher}
import org.gridsim.core.common.Power
import org.gridsim.core.model.{Environment, GridEntityState}
import org.gridsim.core.observability.serialization.SimulationControlUpdate
import org.gridsim.core.simulation.{SimulationConf, SimulationModel, SimulationState, interval}
import org.gridsim.core.solver.PowerFlowSolver

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Success}

object SimulationActor:
  enum Status:
    case IdleStatus, RunningStatus, PausedStatus, StoppedStatus

  val TypeKey: EntityTypeKey[SimulationCommand] = EntityTypeKey("SimulationActor")

  final case class State(
    model: Option[SimulationModel],
    status: Status,
    env: Option[Environment],
    conf: Option[SimulationConf],
    tick: Long = 0L
  )

  def apply(
    persistenceId: PersistenceId,
    entityRefFor: String => EntityRef[EntityCommand],
    tickTimeout: FiniteDuration,
    publisher: SimulationTickPublisher = TelemetryPublisher.NoOpSimulationTickPublisher,
    controlPublisher: SimulationControlPublisher = TelemetryPublisher.NoOpSimulationControlPublisher
  ): Behavior[SimulationCommand] =
    Behaviors.setup { context =>
      implicit val ec: ExecutionContext = context.executionContext

      // Broadcast the current lifecycle/configuration so every connected client can sync its controls
      def publishControl(state: State): Unit =
        state.conf.foreach { conf =>
          controlPublisher.publish(SimulationControlUpdate(state.status.toString, conf.speed, conf.delta))
        }

      EventSourcedBehavior[SimulationCommand, SimulationEvent, State](
        persistenceId,
        emptyState = State(None, IdleStatus, None, None, 0L),
        commandHandler = (state, cmd) => (state, cmd) match
          case (State(None, _, _, _, _), Initialize(model, initState, conf, replyTo)) =>
            Effect
              .persist(Initialized(model, initState.environment, conf))
              .thenRun { newState =>
                publishControl(newState)
                implicit val timeout: Timeout = Timeout(10.seconds)
                implicit val scheduler: Scheduler = context.system.scheduler
                val futures = model.grid.nodes.map { entity =>
                  val entityState = initState.entityStates.get(entity.id)
                  entityRefFor(entity.id).ask(ref => org.gridsim.actor.protocol.EntityProtocol.Initialize(entity, entityState, ref))
                }
                context.pipeToSelf(Future.sequence(futures)) {
                  case Success(_) => org.gridsim.actor.protocol.SimulationProtocol.EntitiesInitialized(replyTo)
                  case Failure(ex) => org.gridsim.actor.protocol.SimulationProtocol.EntitiesInitializationFailed(ex, replyTo)
                }
              }

          case (State(Some(_), IdleStatus, _, _, _), org.gridsim.actor.protocol.SimulationProtocol.EntitiesInitialized(replyTo)) =>
            Effect.none.thenRun(_ => replyTo ! Ack)

          case (State(Some(_), IdleStatus, _, _, _), org.gridsim.actor.protocol.SimulationProtocol.EntitiesInitializationFailed(ex, replyTo)) =>
            context.log.error("Failed to initialize entity actors", ex)
            Effect.none // Maybe we should fail the initialization, but for now just log and do nothing (replyTo will timeout)


          case (State(Some(_), IdleStatus | PausedStatus, _, _, _), Start) =>
            Effect
              .persist(Started)
              .thenRun { newState =>
                publishControl(newState)
                context.self ! TickTimer
              }

          case (State(Some(_), RunningStatus, _, _, _), Pause) =>
            Effect
              .persist(Paused)
              .thenRun(publishControl)

          // Stopping is terminal: pending ticks are dropped and no further command restarts the simulation
          case (State(Some(_), IdleStatus | RunningStatus | PausedStatus, _, _, _), Stop) =>
            Effect
              .persist(Stopped)
              .thenRun(publishControl)

          case (State(Some(model), IdleStatus | PausedStatus, Some(env), Some(conf), currentTick), Step) =>
            Effect
              .none
              .thenRun { _ =>
                implicit val timeout: Timeout = Timeout(tickTimeout)
                implicit val scheduler: Scheduler = context.system.scheduler

                val newEnv = env.advance(conf.delta)
                val futures: Iterable[Future[EntityEvolved]] = model.grid.nodes.map { e =>
                  entityRefFor(e.id).ask(replyTo => Evolve(newEnv, conf.delta, replyTo, currentTick))
                }

                context.pipeToSelf(Future.sequence(futures)) {
                  case Success(results) => EntitiesEvolved(newEnv, results.toList, false)
                  case Failure(ex) => TickFailed(ex)
                }
              }

          case (State(Some(model), RunningStatus, Some(env), Some(conf), currentTick), TickTimer) =>
            Effect
              .none
              .thenRun { _ =>
                implicit val timeout: Timeout = Timeout(tickTimeout)
                implicit val scheduler: Scheduler = context.system.scheduler

                val newEnv = env.advance(conf.delta)
                val futures: Iterable[Future[EntityEvolved]] = model.grid.nodes.map { e =>
                  entityRefFor(e.id).ask(replyTo => Evolve(newEnv, conf.delta, replyTo, currentTick))
                }

                context.pipeToSelf(Future.sequence(futures)) {
                  case Success(results) => EntitiesEvolved(newEnv, results.toList, true)
                  case Failure(ex)      => TickFailed(ex)
                }
              }

          case (State(Some(model), status, _, Some(conf), currentTick), EntitiesEvolved(newEnv, results, scheduleNext)) if status != StoppedStatus =>
            val nextTick = currentTick + 1
            Effect
              .persist(TickAdvanced(newEnv, nextTick))
              .thenRun { _ =>
                val flows = results.map(r => r.id -> r.flow).toMap
                val flowSolver = org.gridsim.core.solver.KirchhoffPowerFlowSolver(model.grid)
                val cableLoads = flowSolver.solve(flows).toMap

                // Publish global tick, environment, and cable load distribution to Kafka
                publisher.publish(currentTick, newEnv, cableLoads, conf.delta)
                if scheduleNext && status == RunningStatus then context.scheduleOnce(conf.speed.interval, context.self, TickTimer)
              }

          case (State(_, _, _, Some(conf), _), TickFailed(ex)) =>
            context.log.warn("Tick failed, retrying", ex)
            Effect
              .none
              .thenRun(_ => context.scheduleOnce(conf.speed.interval, context.self, TickTimer))

          case (State(_, status, _, _, _), org.gridsim.actor.protocol.SimulationProtocol.GetStatus(replyTo)) =>
            replyTo ! status.toString
            Effect.none

          case (State(Some(_), status, _, Some(conf), _), UpdateSpeed(speed)) if status != StoppedStatus =>
            Effect.persist(SpeedUpdated(speed)).thenRun(publishControl)

          case (State(Some(_), status, _, Some(conf), _), UpdateTickDelta(delta)) if status != StoppedStatus =>
            Effect.persist(TickDeltaUpdated(delta)).thenRun(publishControl)

          case _ => Effect.unhandled
        ,
        eventHandler = (state, event) => event match
          case Initialized(model, env, conf) => State(Some(model), IdleStatus, Some(env), Some(conf), 0L)
          case Started                      => state.copy(status = RunningStatus)
          case Paused                       => state.copy(status = PausedStatus)
          case Stopped                      => state.copy(status = StoppedStatus)
          case TickAdvanced(env, tick)      => state.copy(env = Some(env), tick = tick)
          case SpeedUpdated(speed)          => state.copy(conf = state.conf.map(_.copy(speed = speed)))
          case TickDeltaUpdated(delta)      => state.copy(conf = state.conf.map(_.copy(delta = delta)))
      )
        .receiveSignal {
          case (state @ State(Some(_), RunningStatus, _, _, _), RecoveryCompleted) =>
            context.log.info("Recovery completed for the simulation: ", persistenceId.id)
            publishControl(state)
            context.self ! TickTimer
          case (state @ State(Some(_), _, _, _, _), RecoveryCompleted) =>
            publishControl(state)
        }
        .onPersistFailure(
          SupervisorStrategy.restartWithBackoff(
            minBackoff = 200.millis, maxBackoff = 5.seconds, randomFactor = 0.2
          )
        )
    }
