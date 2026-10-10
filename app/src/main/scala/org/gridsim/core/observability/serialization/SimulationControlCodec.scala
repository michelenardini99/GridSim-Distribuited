package org.gridsim.core.observability.serialization

import org.gridsim.core.simulation.SimulationSpeed

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.util.Try

/** Lifecycle and configuration of a simulation, as published on the control topic
  * whenever it changes, so every connected client can stay in sync.
  *
  * @param status the simulation actor status name (e.g. "RunningStatus", "PausedStatus")
  * @param speed the scheduling speed of the simulation
  * @param delta the simulated time advanced at each tick
  */
final case class SimulationControlUpdate(
  status: String,
  speed: SimulationSpeed,
  delta: FiniteDuration
)

/** Plain-text codec for [[SimulationControlUpdate]] in the form `status|speed|deltaMillis`. */
object SimulationControlCodec:

  def toBinary(update: SimulationControlUpdate): Array[Byte] =
    s"${update.status}|${update.speed}|${update.delta.toMillis}".getBytes(StandardCharsets.UTF_8)

  def fromBinary(bytes: Array[Byte]): Either[String, SimulationControlUpdate] =
    new String(bytes, StandardCharsets.UTF_8).split('|') match
      case Array(status, speed, deltaMillis) =>
        Try(SimulationControlUpdate(status, SimulationSpeed.valueOf(speed), deltaMillis.toLong.millis))
          .toEither.left.map(_.getMessage)
      case _ => Left("Malformed simulation control message")
