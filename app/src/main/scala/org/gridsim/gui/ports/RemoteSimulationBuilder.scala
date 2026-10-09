package org.gridsim.gui.ports

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.concurrent.SignallingRef
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.gridsim.core.common.{Energy, Flow}
import org.gridsim.core.model.{Environment, GridEntityState}
import org.gridsim.core.model.network.Cable
import org.gridsim.core.observability.SimulationData
import org.gridsim.core.observability.SimulationData.SimulationSnapshot
import org.gridsim.core.observability.serialization.SimulationDataCodecs
import org.gridsim.core.simulation.{SimulationConf, SimulationController, SimulationControllerState, SimulationModel, SimulationSpeed, SimulationState}
import org.gridsim.gui.model.{ClientConfig, RunningSimulation}
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

    override def setTick(delta: FiniteDuration): Unit = ()
    override def setSpeed(speed: SimulationSpeed): Unit = ()
    override def stepOnce(): SimulationState = currentState
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
    val statusRef = new AtomicReference[SimulationControllerState](PAUSED)
    val confRef = new AtomicReference[SimulationConf](SimulationConf(1.second, Normal))

    val controller = new RemoteSimulationController(apiClient, simId, stateRef, statusRef, confRef)

    // Signals required by UI
    val snapshotSignal = SignallingRef[IO, SimulationSnapshot](
      SimulationSnapshot(env, Map.empty, Map.empty, Map.empty, 1.second)
    ).unsafeRunSync()

    val statsSignal = SignallingRef[IO, StatisticsRegistry.engine.State](
      StatisticsRegistry.engine.initial
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
      consumer.subscribe(java.util.Arrays.asList(entitiesTopic, ticksTopic))

      var entityStates = Map.empty[String, GridEntityState]
      var entityFlows = Map.empty[String, Flow[Energy]]
      var currentStatsState = StatisticsRegistry.engine.initial

      try {
        while (!Thread.currentThread().isInterrupted) {
          val records = consumer.poll(java.time.Duration.ofMillis(100))
          var updatedTick = false
          var latestSnapshot: Option[SimulationSnapshot] = None
          
          records.asScala.foreach { record =>
            if (record.topic() == entitiesTopic) {
              SimulationDataCodecs.telemetryFromBinary(record.value()) match {
                case Right((eid, _, state, flow)) =>
                  entityStates = entityStates + (eid -> state)
                  entityFlows = entityFlows + (eid -> flow)
                case Left(err) => println(s"Failed to decode entity telemetry: $err")
              }
            } else if (record.topic() == ticksTopic) {
              SimulationDataCodecs.gridTickFromBinary(record.value()) match {
                case Right((tick, env, cableLoads, delta)) =>
                  val snapshot: SimulationData.SimulationSnapshot = SimulationData.SimulationSnapshot(env, entityStates, entityFlows, cableLoads, delta)
                  latestSnapshot = Some(snapshot)
                  
                  // Update controller's state references
                  stateRef.set(SimulationState(env, entityStates, entityFlows, cableLoads))
                  confRef.set(SimulationConf(delta, Normal))
                  
                  updatedTick = true
                case Left(err) => println(s"Failed to decode tick telemetry: $err")
              }
            }
          }

          if (updatedTick && latestSnapshot.isDefined) {
            val snap = latestSnapshot.get
            // Push to Signals
            snapshotSignal.set(snap).unsafeRunSync()
            
            // Update stats
            currentStatsState = StatisticsRegistry.engine.step(currentStatsState, snap)
            statsSignal.set(currentStatsState).unsafeRunSync()
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

    RunningSimulation(presetName, model, controller, snapshotSignal, statsSignal)
  }
}
