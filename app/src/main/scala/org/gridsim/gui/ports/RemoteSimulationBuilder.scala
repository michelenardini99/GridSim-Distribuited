package org.gridsim.gui.ports

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.concurrent.SignallingRef
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.gridsim.core.common.{Energy, Flow}
import org.gridsim.core.model.{Environment, GridEntityState}
import org.gridsim.core.model.network.{Cable, ExternalGrid}
import org.gridsim.core.observability.SimulationData
import org.gridsim.core.observability.SimulationData.SimulationSnapshot
import org.gridsim.core.observability.serialization.{SimulationControlCodec, SimulationDataCodecs}
import org.gridsim.core.simulation.{SimulationConf, SimulationController, SimulationControllerState, SimulationModel, SimulationSpeed, SimulationState}
import org.gridsim.gui.model.{ClientConfig, ControlState, RunningSimulation}
import org.gridsim.statistics.StatisticsRegistry
import org.gridsim.core.simulation.SimulationControllerState.{PAUSED, RUNNING}
import org.gridsim.core.simulation.SimulationSpeed.Normal

import java.time.LocalDateTime
import java.util.{Collections, Properties, UUID}
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters.*

object RemoteSimulationBuilder {

  /** Creates a RemoteSimulationController backed by HTTP API calls. */
  class RemoteSimulationController(
      apiClient: SimulationApiClient,
      simId: String,
      stateRef: AtomicReference[SimulationState],
      statusRef: AtomicReference[SimulationControllerState],
      confRef: AtomicReference[SimulationConf]
  ) extends SimulationController {
    override def currentState: SimulationState = stateRef.get()
    override def simulationControllerState: SimulationControllerState = statusRef.get()
    override def configuration: SimulationConf = confRef.get()

    override def start(): Unit = {
      apiClient.startSimulation(simId)
      statusRef.set(RUNNING)
    }

    override def stop(): Unit = {
      apiClient.stopSimulation(simId)
      statusRef.set(PAUSED)
    }

    override def pause(): Unit = {
      apiClient.pauseSimulation(simId)
      statusRef.set(PAUSED)
    }

    override def resume(): Unit = start()

    override def setTick(delta: FiniteDuration): Unit = {
      apiClient.setSimulationTickDuration(simId, delta)
      confRef.updateAndGet(_.copy(delta = delta))
    }
    override def setSpeed(speed: SimulationSpeed): Unit = {
      apiClient.setSimulationSpeed(simId, speed)
      confRef.updateAndGet(_.copy(speed = speed))
    }
    override def stepOnce(): SimulationState = {
      apiClient.stepSimulation(simId)
      currentState
    }
  }

  def build(
      simId: String,
      presetName: String,
      model: SimulationModel,
      apiClient: SimulationApiClient,
      config: ClientConfig
  ): RunningSimulation = {
    // Initial empty state
    val env = Environment(LocalDateTime.now(), 0.seconds)
    val initialState = SimulationState(env, Map.empty, Map.empty, Map.empty)

    val stateRef = new AtomicReference[SimulationState](initialState)

    // Fetch current simulation status from API
    val initialStatus = try {
      val statusStr = scala.concurrent.Await.result(apiClient.getSimulationStatus(simId), 5.seconds)
      if (statusStr == "RunningStatus") RUNNING else PAUSED
    } catch {
      case e: Exception => PAUSED
    }

    val statusRef = new AtomicReference[SimulationControllerState](initialStatus)
    val confRef = new AtomicReference[SimulationConf](SimulationConf(1.second, Normal))

    val controller = new RemoteSimulationController(apiClient, simId, stateRef, statusRef, confRef)

    // Signals required by UI
    val snapshotSignal = SignallingRef[IO, SimulationSnapshot](
      SimulationSnapshot(env, Map.empty, Map.empty, Map.empty, 1.second)
    ).unsafeRunSync()

    val statsSignal = SignallingRef[IO, StatisticsRegistry.engine.State](
      StatisticsRegistry.engine.initial
    ).unsafeRunSync()

    val controlSignal = SignallingRef[IO, ControlState](
      ControlState(initialStatus, confRef.get())
    ).unsafeRunSync()

    // Spawn a background thread for Kafka Consumption
    val thread = new Thread(() => {
      val props = new Properties()
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers)
      props.put(ConsumerConfig.GROUP_ID_CONFIG, s"gui-consumer-${UUID.randomUUID()}")
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

      val consumer = new KafkaConsumer[String, Array[Byte]](props)
      val entitiesTopic = s"grid.entities.$simId"
      val ticksTopic = s"grid.ticks.$simId"
      val controlTopic = s"grid.control.$simId"
      consumer.subscribe(java.util.Arrays.asList(entitiesTopic, ticksTopic, controlTopic))

      val entityQueues = scala.collection.mutable.Map.empty[String, scala.collection.mutable.Queue[(Long, GridEntityState, Flow[Energy])]]
      val knownEntityIds: Set[String] = model.grid.nodes.filterNot(_.isInstanceOf[ExternalGrid]).map(_.id).toSet
      var entityStates = Map.empty[String, GridEntityState]
      var entityFlows = Map.empty[String, Flow[Energy]]
      var entityAppliedTick = Map.empty[String, Long]
      val pendingTicks = scala.collection.mutable.TreeMap.empty[Long, (Environment, Map[Cable, Energy], FiniteDuration)]
      var currentStatsState = StatisticsRegistry.engine.initial

      val publishInterval = 100.millis
      var lastPublishAt = System.nanoTime() - publishInterval.toNanos

      try {
        while (!Thread.currentThread().isInterrupted) {
          val records = consumer.poll(java.time.Duration.ofMillis(100))

          records.asScala.foreach { record =>
            if (record.topic() == entitiesTopic) {
              SimulationDataCodecs.telemetryFromBinary(record.value()) match {
                case Right((eid, tick, state, flow)) =>
                  entityQueues.getOrElseUpdate(eid, scala.collection.mutable.Queue.empty).enqueue((tick, state, flow))
                case Left(err) => println(s"Failed to decode entity telemetry: $err")
              }
            } else if (record.topic() == ticksTopic) {
              SimulationDataCodecs.gridTickFromBinary(record.value()) match {
                case Right((tick, env, cableLoads, delta)) =>
                  pendingTicks.update(tick, (env, cableLoads, delta))
                case Left(err) => println(s"Failed to decode tick telemetry: $err")
              }
            } else if (record.topic() == controlTopic) {
              SimulationControlCodec.fromBinary(record.value()) match {
                case Right(update) =>
                  // The backend is the source of truth: overwrite the local status and configuration
                  val status = if (update.status == "RunningStatus") RUNNING else PAUSED
                  statusRef.set(status)
                  val conf = confRef.updateAndGet(_.copy(speed = update.speed, delta = update.delta))
                  controlSignal.set(ControlState(status, conf)).unsafeRunSync()
                case Left(err) => println(s"Failed to decode control message: $err")
              }
            }
          }

          var draining = true
          while (draining) {
            pendingTicks.headOption match {
              case Some((tick, (env, cableLoads, delta))) =>
                // Advance each known entity's applied state up to (but not beyond) `tick`.
                val allReady = knownEntityIds.forall { id =>
                  val q = entityQueues.getOrElseUpdate(id, scala.collection.mutable.Queue.empty)
                  while (q.nonEmpty && q.front._1 <= tick) {
                    val (t, state, flow) = q.dequeue()
                    entityStates = entityStates + (id -> state)
                    entityFlows = entityFlows + (id -> flow)
                    entityAppliedTick = entityAppliedTick + (id -> t)
                  }
                  entityAppliedTick.get(id).exists(_ >= tick)
                }

                if (allReady) {
                  pendingTicks.remove(tick)

                  val snap: SimulationData.SimulationSnapshot = SimulationData.SimulationSnapshot(env, entityStates, entityFlows, cableLoads, delta)

                  stateRef.set(SimulationState(env, entityStates, entityFlows, cableLoads))
                  confRef.updateAndGet(_.copy(delta = delta))

                  currentStatsState = StatisticsRegistry.engine.step(currentStatsState, snap)

                  val now = System.nanoTime()
                  if (pendingTicks.isEmpty || (now - lastPublishAt) >= publishInterval.toNanos) {
                    snapshotSignal.set(snap).unsafeRunSync()
                    statsSignal.set(currentStatsState).unsafeRunSync()
                    lastPublishAt = now
                  }
                } else {
                  draining = false
                }
              case None =>
                draining = false
            }
          }
        }
      } catch {
        case _: InterruptedException => // Shutting down
        case e: Exception => e.printStackTrace()
      } finally {
        consumer.close()
      }
    })

    thread.setDaemon(true)
    thread.start()

    RunningSimulation(presetName, model, controller, snapshotSignal, statsSignal, Some(controlSignal))
  }
}
