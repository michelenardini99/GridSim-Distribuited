package org.gridsim.core.simulation

import org.gridsim.core.simulation.SimulationControllerState.{PAUSED, RUNNING}
import org.gridsim.core.simulation.scheduling.{ScheduledTask, Scheduler}

import java.util.concurrent.atomic.AtomicReference
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.gridsim.core.observability.DataDispatcher
import org.gridsim.core.simulation.SimulationSpeed.{Normal, Slow, Speed, UltraSpeed}

import scala.concurrent.duration.{DurationInt, FiniteDuration}

case class SimulationConf(
  delta: FiniteDuration,
  speed: SimulationSpeed = Normal
) extends org.gridsim.actor.protocol.Serializable

enum SimulationSpeed extends org.gridsim.actor.protocol.Serializable:
  case Slow
  case Normal
  case Speed
  case UltraSpeed

extension (s: SimulationSpeed)
  def interval: FiniteDuration =
    s match
      case Slow => 2.seconds
      case Normal => 1.second
      case Speed => 500.milliseconds
      case UltraSpeed => 100.milliseconds


/** Lifecycle state of a [[SimulationRunner]]. */
enum SimulationControllerState:
  /** The runner is executing scheduled simulation ticks. */
  case RUNNING

  /** The runner keeps its current state but skips scheduled ticks. */
  case PAUSED

/** Default scheduled implementation of [[SimulationRunner]].
  *
  * The runner keeps the current [[SimulationState]] in an atomic reference and
  * periodically advances it by invoking [[SimulationEngine.step]]. The engine
  * remains responsible only for the pure transition from one snapshot to the
  * next; this class owns scheduling and lifecycle control.
  *
  * @param engine
  *   pure simulation engine used to compute each tick
  * @param state
  *   initial simulation snapshot
  * @param scheduler
  *   the entity responsible to schedule the step task.
  * @param conf
  *   the simulation configuration
  */
final case class DefaultSimulationController(
    engine: SimulationEngine,
    state: SimulationState,
    scheduler: Scheduler,
    conf: SimulationConf,
    dispatcher: Option[DataDispatcher[IO]] = None
) extends SimulationController:

  private val stateRef = AtomicReference[SimulationState](state)
  private val simulationControllerStateRef =
    AtomicReference[SimulationControllerState](PAUSED)
  private val activeTaskRef = AtomicReference[Option[ScheduledTask]](None)
  private val confRef = AtomicReference[SimulationConf](conf)
  private val lifecycleLock = Object()

  /** Returns the latest simulation snapshot.
    *
    * @return
    *   the state currently stored by the runner
    */
  def currentState: SimulationState = stateRef.get()

  /** Returns the current lifecycle state.
    *
    * @return
    *   [[SimulationRunnerState.RUNNING]] when ticks are active,
    *   [[SimulationRunnerState.PAUSED]] otherwise
    */
  def simulationControllerState: SimulationControllerState =
    simulationControllerStateRef.get()

  override def configuration: SimulationConf =
    confRef.get()

  /** Starts scheduling simulation ticks.
    *
    * One tick is scheduled after the currently configured speed interval. Each
    * completed tick schedules exactly one successor using the latest speed.
    */
  override def start(): Unit =
    lifecycleLock.synchronized {
      if simulationControllerStateRef.compareAndSet(PAUSED, RUNNING) then
        scheduleNextTick()
    }

  /** Pauses scheduled execution.
    *
    * Already scheduled tasks may still wake up, but they will not advance the
    * simulation while the runner state is [[SimulationRunnerState.PAUSED]].
    */
  override def pause(): Unit =
    lifecycleLock.synchronized {
      if simulationControllerStateRef.get() == RUNNING then
        simulationControllerStateRef.set(PAUSED)
        cancelActiveTask()
    }

  /** Stops the scheduler and releases its thread.
    *
    * After shutdown, the current scheduler cannot be started again.
    */
  override def stop(): Unit =
    lifecycleLock.synchronized {
      simulationControllerStateRef.set(PAUSED)
      cancelActiveTask()
      scheduler.stop()
    }

  /** Resumes periodic execution by delegating to [[start]]. */
  override def resume(): Unit =
    start()

  /** Advances the current state by one engine tick.
    *
    * @return
    *   the updated simulation state
    */
  override def stepOnce(): SimulationState =
    val conf = confRef.get()
    val newState = stateRef.updateAndGet { current =>
      engine.step(current, conf.delta)
    }
    dispatcher.foreach { d =>
      d.dispatch(newState, conf.delta).unsafeRunSync()
    }
    newState

  override def setTick(delta: FiniteDuration): Unit =
    confRef.updateAndGet(_.copy(delta = delta))

  override def setSpeed(speed: SimulationSpeed): Unit =
    confRef.updateAndGet(_.copy(speed = speed))

  /** Executes one scheduled tick and only then arms its single successor. */
  private def runScheduledTick(): Unit =
    lifecycleLock.synchronized {
      activeTaskRef.set(None)
      if simulationControllerStateRef.get() == RUNNING then
        stepOnce()
        if simulationControllerStateRef.get() == RUNNING then
          scheduleNextTick()
    }

  /** Must be called while holding `lifecycleLock`. */
  private def scheduleNextTick(): Unit =
    val delay = confRef.get().speed.interval
    val task = scheduler.scheduleOnce(() => runScheduledTick(), delay)
    activeTaskRef.set(Some(task))

  private def cancelActiveTask(): Unit =
    activeTaskRef.getAndSet(None).foreach(_.cancel())
