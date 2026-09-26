package org.gridsim.core.observability.serialization

import org.gridsim.core.common.{Energy, Flow, Power, kw, kwh}
import org.gridsim.core.model.{Environment, SolarPanelState}
import org.gridsim.core.model.house.HouseState
import org.gridsim.core.model.network.{Cable, CableConnections}
import org.gridsim.core.model.storage.battery.BatteryState
import org.gridsim.core.observability.SimulationData
import org.gridsim.core.observability.serialization.SimulationDataCodecs.*
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.*

@RunWith(classOf[JUnitRunner])
class SimulationDataCodecsSpec extends AnyFlatSpec with Matchers:

  private val testStartTime = LocalDateTime.of(2026, 9, 23, 12, 0, 0)
  private val testEnv = Environment(testStartTime, 30.minutes)

  private val solarState = SolarPanelState("solar-1", efficiency = 0.195)
  private val batteryState = BatteryState("battery-1", currentCharge = 12.5.kwh)
  private val houseState = HouseState(
    entityId = "house-1",
    componentStates = List(solarState, batteryState)
  )

  private val cable1 = Cable(CableConnections("node-A", "node-B"), maxCapacity = 50.0.kw)
  private val cable2 = Cable(CableConnections("node-B", "node-C"), maxCapacity = 100.0.kw)

  "SimulationDataCodecs" should "serialize and deserialize EnvironmentData correctly" in:
    val domainData: SimulationData = SimulationData.EnvironmentData(testEnv)
    val protoMsg = domainData.toProtoMessage
    val decoded = protoMsg.toDomainData

    decoded shouldBe Right(domainData)

    // Test binary roundtrip
    val bytes = domainData.toBinary
    val fromBytes = SimulationDataCodecs.fromBinary(bytes)
    fromBytes shouldBe Right(domainData)

  it should "serialize and deserialize EntityStatesData with polymorphic states correctly" in:
    val states = Map(
      "solar-1" -> solarState,
      "battery-1" -> batteryState,
      "house-1" -> houseState
    )
    val domainData: SimulationData = SimulationData.EntityStatesData(states)
    val protoMsg = domainData.toProtoMessage
    val decoded = protoMsg.toDomainData

    decoded shouldBe Right(domainData)

    val bytes = domainData.toBinary
    val fromBytes = SimulationDataCodecs.fromBinary(bytes)
    fromBytes shouldBe Right(domainData)

  it should "serialize and deserialize EntityFlowsData with all Flow types" in:
    val flows: Map[String, Flow[Energy]] = Map(
      "producer-1" -> Flow.Surplus(15.2.kwh),
      "consumer-1" -> Flow.Deficit(7.8.kwh),
      "neutral-1"  -> Flow.Balanced
    )
    val domainData: SimulationData = SimulationData.EntityFlowsData(flows)
    val protoMsg = domainData.toProtoMessage
    val decoded = protoMsg.toDomainData

    decoded shouldBe Right(domainData)

    val bytes = domainData.toBinary
    val fromBytes = SimulationDataCodecs.fromBinary(bytes)
    fromBytes shouldBe Right(domainData)

  it should "serialize and deserialize CableLoadsData correctly" in:
    val loads: Map[Cable, Energy] = Map(
      cable1 -> 22.4.kwh,
      cable2 -> 85.1.kwh
    )
    val domainData: SimulationData = SimulationData.CableLoadsData(loads)
    val protoMsg = domainData.toProtoMessage
    val decoded = protoMsg.toDomainData

    decoded shouldBe Right(domainData)

    val bytes = domainData.toBinary
    val fromBytes = SimulationDataCodecs.fromBinary(bytes)
    fromBytes shouldBe Right(domainData)

  it should "serialize and deserialize SimulationSnapshot with complete state correctly" in:
    val snapshot = SimulationData.SimulationSnapshot(
      environment = testEnv,
      entityStates = Map("house-1" -> houseState),
      entityFlows = Map("house-1" -> Flow.Deficit(3.5.kwh)),
      cableLoads = Map(cable1 -> 3.5.kwh),
      delta = 1.minute
    )

    val protoMsg = snapshot.toProtoMessage
    val decoded = protoMsg.toDomainData

    decoded shouldBe Right(snapshot)

    val bytes = snapshot.toBinary
    val fromBytes = SimulationDataCodecs.fromBinary(bytes)
    fromBytes shouldBe Right(snapshot)
