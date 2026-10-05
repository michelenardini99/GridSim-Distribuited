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
import org.gridsim.actor.protocol.SimulationProtocol.{Ack, EntitiesEvolved, Initialize, Initialized, Pause, Paused, SimulationCommand, SimulationEvent, Start, Started, Stopped, TickAdvanced, TickFailed, TickTimer}
import org.gridsim.core.common.Power
import org.gridsim.core.model.Environment
import org.gridsim.core.simulation.{SimulationConf, SimulationModel, SimulationState}
import org.gridsim.core.solver.PowerFlowSolver

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Success}

object SimulationActor:

  enum Status:
    case IdleStatus, RunningStatus, PausedStatus, StoppedStatus

  val TypeKey: EntityTypeKey[SimulationCommand] = EntityTypeKey("SimulationActor")

  final case class State(model: Option[SimulationModel], status: Status, env: Option[Environment], conf: Option[SimulationConf])

  def apply(
    persistenceId: PersistenceId,
    entityRefFor: String => EntityRef[EntityCommand],
    flowSolver: PowerFlowSolver,
    tickTimeout: FiniteDuration
  ): Behavior[SimulationCommand] =
    Behaviors.setup { context =>
      implicit val ec: ExecutionContext = context.executionContext
      EventSourcedBehavior[SimulationCommand, SimulationEvent, State](
        persistenceId,
        emptyState = State(None, IdleStatus, None, None),
        commandHandler = (state, cmd) => (state, cmd) match
          case (State(None, _, _, _), Initialize(model, initState, conf, replyTo)) =>
            Effect
              .persist(Initialized(model, initState.environment, conf))
              .thenRun(_ => replyTo ! Ack)

          case (State(Some(_), IdleStatus | PausedStatus, _, _), Start) =>
            Effect
              .persist(Started)
              .thenRun(_ => context.self ! TickTimer)

          case (State(Some(_), RunningStatus, _, _), Pause) =>
            Effect
              .persist(Paused)

          case (State(Some(model), RunningStatus, Some(env), Some(conf)), TickTimer) =>
            Effect
              .none
              .thenRun{ _ =>
                implicit val timeout: Timeout = Timeout(tickTimeout)
                implicit val scheduler: Scheduler = context.system.scheduler

                val newEnv = env.advance(conf.delta)
                val futures: Iterable[Future[EntityEvolved]] = model.grid.nodes.map { e =>
                  entityRefFor(e.id).ask(replyTo => Evolve(newEnv, conf.delta, replyTo))
                }

                context.pipeToSelf(Future.sequence(futures)) {
                  case Success(results) => EntitiesEvolved(newEnv, results.toList)
                  case Failure(ex) => TickFailed(ex)
                }
              }

          case(State(Some(model), RunningStatus, _, Some(conf)), EntitiesEvolved(newEnv, results)) =>
            Effect
              .persist(TickAdvanced(newEnv))
              .thenRun{ _ =>
                val flows = results.map(r => r.id -> r.flow).toMap
                val cableLoads = flowSolver.solve(flows).toMap

                val snapshot = SimulationState(newEnv, results.map(r => r.id -> r.state).toMap, flows, cableLoads)
                //here send to subscriber
                context.scheduleOnce(tickTimeout, context.self, TickTimer)
              }

          case (_, TickFailed(ex)) =>
            context.log.warn("Tick failed, retrying", ex)
            Effect
              .none
              .thenRun(_ => context.scheduleOnce(tickTimeout, context.self, TickTimer))

          case _ => Effect.unhandled
        ,
        eventHandler = (state, event) =>  event match
          case Initialized(model, env, conf) => State(Some(model), IdleStatus, Some(env), Some(conf))
          case Started => state.copy(status = RunningStatus)
          case Paused => state.copy(status = PausedStatus)
          case Stopped => state.copy(status = StoppedStatus)
          case TickAdvanced(env) => state.copy(env = Some(env))
      )
        .receiveSignal {
          case (State(Some(_), RunningStatus, _, _), RecoveryCompleted) =>
            context.log.info("Recovery completed for the simulation: ", persistenceId.id)
            context.self ! TickTimer
        }
        .onPersistFailure(
          SupervisorStrategy.restartWithBackoff(
            minBackoff = 200.millis, maxBackoff = 5.seconds, randomFactor = 0.2
          )
        )
    }
