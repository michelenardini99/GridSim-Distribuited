package org.gridsim.core.observability.serialization

import cats.syntax.all.*
import org.gridsim.core.common.{Energy, Flow, Power}
import org.gridsim.core.model.{Environment, GridEntityState, SolarPanelState}
import org.gridsim.core.model.house.HouseState
import org.gridsim.core.model.network.{Cable, CableConnections}
import org.gridsim.core.model.storage.battery.BatteryState
import org.gridsim.core.observability.SimulationData
import org.gridsim.protocol.v1.common.{CableConnectionsMessage, CableMessage, FlowMessage}
import org.gridsim.protocol.v1.entity_state.*
import org.gridsim.protocol.v1.simulation_data.*

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

/** Bidirectional codecs translating between the core domain [[SimulationData]]
  * ADT models and the Protobuf / ScalaPB wire representations.
  */
object SimulationDataCodecs:

  // --- Topology & Flow ---

  def toProto(connections: CableConnections): CableConnectionsMessage =
    CableConnectionsMessage(n1 = connections.n1, n2 = connections.n2)

  def toDomain(msg: CableConnectionsMessage): CableConnections =
    CableConnections(n1 = msg.n1, n2 = msg.n2)

  def toProto(cable: Cable): CableMessage =
    CableMessage(
      connections = Some(toProto(cable.connections)),
      maxCapacityKw = cable.maxCapacity.toDouble
    )

  def toDomain(msg: CableMessage): Either[String, Cable] =
    for {
      connMsg <- msg.connections.toRight("Missing connections in CableMessage")
    } yield Cable(
      connections = toDomain(connMsg),
      maxCapacity = Power(msg.maxCapacityKw)
    )

  def toProto(flow: Flow[Energy]): FlowMessage = flow match
    case Flow.Surplus(amount) =>
      FlowMessage(flow = FlowMessage.Flow.SurplusKwh(amount.toDouble))
    case Flow.Deficit(amount) =>
      FlowMessage(flow = FlowMessage.Flow.DeficitKwh(amount.toDouble))
    case Flow.Balanced =>
      FlowMessage(flow = FlowMessage.Flow.Balanced(true))

  def toDomain(msg: FlowMessage): Either[String, Flow[Energy]] = msg.flow match
    case FlowMessage.Flow.SurplusKwh(v) => Right(Flow.Surplus(Energy(v)))
    case FlowMessage.Flow.DeficitKwh(v) => Right(Flow.Deficit(Energy(v)))
    case FlowMessage.Flow.Balanced(_)   => Right(Flow.Balanced)
    case FlowMessage.Flow.Empty         => Left("Empty flow in FlowMessage")

  // --- GridEntityState ---

  def toProto(state: GridEntityState): GridEntityStateMessage = state match
    case SolarPanelState(id, efficiency) =>
      GridEntityStateMessage(
        entityId = id,
        state = GridEntityStateMessage.State.SolarPanel(
          SolarPanelStateMessage(efficiency)
        )
      )
    case BatteryState(id, charge) =>
      GridEntityStateMessage(
        entityId = id,
        state = GridEntityStateMessage.State.Battery(
          BatteryStateMessage(charge.toDouble)
        )
      )
    case HouseState(id, components) =>
      GridEntityStateMessage(
        entityId = id,
        state = GridEntityStateMessage.State.House(
          HouseStateMessage(componentStates = components.map(toProto).toSeq)
        )
      )
    case other =>
      throw new IllegalArgumentException(
        s"Unsupported GridEntityState subtype for Protobuf serialization: ${other.getClass.getName}"
      )

  def toDomain(msg: GridEntityStateMessage): Either[String, GridEntityState] =
    msg.state match
      case GridEntityStateMessage.State.SolarPanel(sp) =>
        Right(SolarPanelState(entityId = msg.entityId, efficiency = sp.efficiency))
      case GridEntityStateMessage.State.Battery(b) =>
        Right(
          BatteryState(
            entityId = msg.entityId,
            currentCharge = Energy(b.currentChargeKwh)
          )
        )
      case GridEntityStateMessage.State.House(h) =>
        h.componentStates.toList.traverse(toDomain).map { components =>
          HouseState(entityId = msg.entityId, componentStates = components)
        }
      case GridEntityStateMessage.State.Empty =>
        Left(s"Empty state for GridEntityStateMessage with entityId: ${msg.entityId}")

  // --- Environment ---

  def toProto(env: Environment): EnvironmentMessage =
    EnvironmentMessage(
      startDateTimeIso = env.startDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
      timeNanos = env.time.toNanos
    )

  def toDomain(msg: EnvironmentMessage): Either[String, Environment] =
    try
      val start = LocalDateTime.parse(
        msg.startDateTimeIso,
        DateTimeFormatter.ISO_LOCAL_DATE_TIME
      )
      val time = FiniteDuration(msg.timeNanos, TimeUnit.NANOSECONDS)
      Right(Environment(start, time))
    catch
      case ex: Exception =>
        Left(s"Failed to parse EnvironmentMessage: ${ex.getMessage}")

  // --- SimulationData ADT ---

  def toProto(data: SimulationData): SimulationDataMessage = data match
    case SimulationData.EnvironmentData(env) =>
      SimulationDataMessage(
        data = SimulationDataMessage.Data.EnvironmentData(
          EnvironmentDataMessage(environment = Some(toProto(env)))
        )
      )
    case SimulationData.EntityStatesData(states) =>
      SimulationDataMessage(
        data = SimulationDataMessage.Data.EntityStatesData(
          EntityStatesDataMessage(states = states.view.mapValues(toProto).toMap)
        )
      )
    case SimulationData.EntityFlowsData(flows) =>
      SimulationDataMessage(
        data = SimulationDataMessage.Data.EntityFlowsData(
          EntityFlowsDataMessage(flows = flows.view.mapValues(toProto).toMap)
        )
      )
    case SimulationData.CableLoadsData(loads) =>
      val entries = loads.map { case (cable, energy) =>
        CableLoadEntry(cable = Some(toProto(cable)), loadKwh = energy.toDouble)
      }.toSeq
      SimulationDataMessage(
        data = SimulationDataMessage.Data.CableLoadsData(
          CableLoadsDataMessage(loads = entries)
        )
      )
    case SimulationData.SimulationSnapshot(env, states, flows, loads, delta) =>
      val cableEntries = loads.map { case (cable, energy) =>
        CableLoadEntry(cable = Some(toProto(cable)), loadKwh = energy.toDouble)
      }.toSeq
      SimulationDataMessage(
        data = SimulationDataMessage.Data.SimulationSnapshot(
          SimulationSnapshotMessage(
            environment = Some(toProto(env)),
            entityStates = states.view.mapValues(toProto).toMap,
            entityFlows = flows.view.mapValues(toProto).toMap,
            cableLoads = cableEntries,
            deltaNanos = delta.toNanos
          )
        )
      )

  def toDomain(msg: SimulationDataMessage): Either[String, SimulationData] =
    msg.data match
      case SimulationDataMessage.Data.EnvironmentData(envDataMsg) =>
        for {
          envMsg <- envDataMsg.environment.toRight("Missing environment in EnvironmentDataMessage")
          env <- toDomain(envMsg)
        } yield SimulationData.EnvironmentData(env)

      case SimulationDataMessage.Data.EntityStatesData(statesMsg) =>
        statesMsg.states.toList
          .traverse { case (k, v) => toDomain(v).map(st => (k, st)) }
          .map(pairs => SimulationData.EntityStatesData(pairs.toMap))

      case SimulationDataMessage.Data.EntityFlowsData(flowsMsg) =>
        flowsMsg.flows.toList
          .traverse { case (k, v) => toDomain(v).map(fl => (k, fl)) }
          .map(pairs => SimulationData.EntityFlowsData(pairs.toMap))

      case SimulationDataMessage.Data.CableLoadsData(cableLoadsMsg) =>
        cableLoadsMsg.loads.toList
          .traverse { entry =>
            for {
              cMsg <- entry.cable.toRight("Missing cable in CableLoadEntry")
              cable <- toDomain(cMsg)
            } yield (cable, Energy(entry.loadKwh))
          }
          .map(pairs => SimulationData.CableLoadsData(pairs.toMap))

      case SimulationDataMessage.Data.SimulationSnapshot(snapMsg) =>
        for {
          envMsg <- snapMsg.environment.toRight("Missing environment in SimulationSnapshotMessage")
          env <- toDomain(envMsg)
          states <- snapMsg.entityStates.toList.traverse { case (k, v) =>
            toDomain(v).map(st => (k, st))
          }.map(_.toMap)
          flows <- snapMsg.entityFlows.toList.traverse { case (k, v) =>
            toDomain(v).map(fl => (k, fl))
          }.map(_.toMap)
          loads <- snapMsg.cableLoads.toList.traverse { entry =>
            for {
              cMsg <- entry.cable.toRight("Missing cable in CableLoadEntry")
              cable <- toDomain(cMsg)
            } yield (cable, Energy(entry.loadKwh))
          }.map(_.toMap)
          delta = FiniteDuration(snapMsg.deltaNanos, TimeUnit.NANOSECONDS)
        } yield SimulationData.SimulationSnapshot(env, states, flows, loads, delta)

      case SimulationDataMessage.Data.Empty =>
        Left("Empty SimulationDataMessage payload")

  // --- Hybrid Telemetry Codecs ---

  def toProtoTelemetry(
      entityId: String,
      tick: Long,
      state: GridEntityState,
      flow: Flow[Energy]
  ): EntityTelemetryMessage =
    EntityTelemetryMessage(
      entityId = entityId,
      tick = tick,
      state = Some(toProto(state)),
      flow = Some(toProto(flow))
    )

  def toDomainTelemetry(
      msg: EntityTelemetryMessage
  ): Either[String, (String, Long, GridEntityState, Flow[Energy])] =
    for {
      stateMsg <- msg.state.toRight("Missing state in EntityTelemetryMessage")
      state <- toDomain(stateMsg)
      flowMsg <- msg.flow.toRight("Missing flow in EntityTelemetryMessage")
      flow <- toDomain(flowMsg)
    } yield (msg.entityId, msg.tick, state, flow)

  def toProtoGridTick(
      tick: Long,
      env: Environment,
      cableLoads: Map[Cable, Energy],
      delta: FiniteDuration
  ): GridTickMessage =
    val entries = cableLoads.map { case (cable, energy) =>
      CableLoadEntry(cable = Some(toProto(cable)), loadKwh = energy.toDouble)
    }.toSeq
    GridTickMessage(
      tick = tick,
      environment = Some(toProto(env)),
      cableLoads = Some(CableLoadsDataMessage(loads = entries)),
      deltaNanos = delta.toNanos
    )

  def toDomainGridTick(
      msg: GridTickMessage
  ): Either[String, (Long, Environment, Map[Cable, Energy], FiniteDuration)] =
    for {
      envMsg <- msg.environment.toRight("Missing environment in GridTickMessage")
      env <- toDomain(envMsg)
      loadsMsg <- msg.cableLoads.toRight("Missing cable_loads in GridTickMessage")
      loads <- loadsMsg.loads.toList.traverse { entry =>
        for {
          cMsg <- entry.cable.toRight("Missing cable in CableLoadEntry")
          cable <- toDomain(cMsg)
        } yield (cable, Energy(entry.loadKwh))
      }.map(_.toMap)
      delta = FiniteDuration(msg.deltaNanos, TimeUnit.NANOSECONDS)
    } yield (msg.tick, env, loads, delta)

  // --- Binary Serialization Helpers for Kafka ---

  extension (data: SimulationData)
    def toProtoMessage: SimulationDataMessage = toProto(data)
    def toBinary: Array[Byte] = toProto(data).toByteArray

  extension (msg: SimulationDataMessage)
    def toDomainData: Either[String, SimulationData] = toDomain(msg)

  def fromBinary(bytes: Array[Byte]): Either[String, SimulationData] =
    scala.util.Try(SimulationDataMessage.parseFrom(bytes)).toEither
      .left.map(err => s"Protobuf parse error: ${err.getMessage}")
      .flatMap(toDomain)

  def telemetryToBinary(
      entityId: String,
      tick: Long,
      state: GridEntityState,
      flow: Flow[Energy]
  ): Array[Byte] =
    toProtoTelemetry(entityId, tick, state, flow).toByteArray

  def telemetryFromBinary(
      bytes: Array[Byte]
  ): Either[String, (String, Long, GridEntityState, Flow[Energy])] =
    scala.util.Try(EntityTelemetryMessage.parseFrom(bytes)).toEither
      .left.map(err => s"Protobuf parse error: ${err.getMessage}")
      .flatMap(toDomainTelemetry)

  def gridTickToBinary(
      tick: Long,
      env: Environment,
      cableLoads: Map[Cable, Energy],
      delta: FiniteDuration
  ): Array[Byte] =
    toProtoGridTick(tick, env, cableLoads, delta).toByteArray

  def gridTickFromBinary(
      bytes: Array[Byte]
  ): Either[String, (Long, Environment, Map[Cable, Energy], FiniteDuration)] =
    scala.util.Try(GridTickMessage.parseFrom(bytes)).toEither
      .left.map(err => s"Protobuf parse error: ${err.getMessage}")
      .flatMap(toDomainGridTick)

