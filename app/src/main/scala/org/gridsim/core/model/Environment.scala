package org.gridsim.core.model

import org.gridsim.core.common.{GeographicPoint, Irradiance}
import org.gridsim.core.common.Temperatures.{AnyTemperature, Temperature}

import java.time.LocalDateTime
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** Weather conditions at a geographic location and at a given tick. */
trait WeatherConditions:
  /** incident solar irradiance (W/m²), 0 at night */
  def irradiance: Irradiance
  /** air temperature at the location */
  def temperature: AnyTemperature

private final case class WeatherConditionsImpl(irradiance: Irradiance, temperature: AnyTemperature)
  extends WeatherConditions

final case class EnvironmentSnapshot(
  startDateTime: LocalDateTime,
  time: FiniteDuration,
  weather: Map[GeographicPoint, WeatherConditions]
)



object WeatherConditions:
  def apply(irradiance: Irradiance, temperature: AnyTemperature): WeatherConditions =
    WeatherConditionsImpl(irradiance, temperature)

/**
 * Represents the state of the world outside the micro-grid at a given instant.
 *
 * The environment is immutable: each tick produces a new [[Environment]]
 * through [[advance]], without mutating the current state. Grid components
 * query the environment to calculate their production or consumption for the
 * current tick.
 */
trait Environment:
  /** The calendar moment the simulation started at (tick 0). */
  def startDateTime: LocalDateTime

  /** The current simulation time instant. */
  def time: FiniteDuration

  /** The current calendar moment: [[startDateTime]] advanced by [[time]]. */
  final def currentDateTime: LocalDateTime =
    startDateTime.plusNanos(time.toNanos)

  /** Current hour of day, normalized to the range 0-23. */
  final def hourOfDay: Int =
    Math.floorMod(time.toHours, 24L).toInt

  /**
   * Returns the weather conditions at the geographic location [[point]]
   * at the given [[time]].
   *
   * The location influences the result because irradiance and wind conditions
   * may vary across space (e.g. solar panels on a specific rooftop, a wind farm
   * located in a valley).
   *
   * @param point geographic position of the requesting component
   * @return localized weather conditions
   */
  def weather(point: GeographicPoint): WeatherConditions

  /**
   * Produces a new [[Environment]] advanced by one simulation tick.
   *
   * The tick duration is defined externally (e.g. as a [[FiniteDuration]]
   * in the simulation configuration) and is not part of the environment's
   * contract: each call to [[advance]] corresponds to a single simulation
   * step, regardless of its actual real-world duration.
   *
   * @param delta the time to elapse to go to next environment
   * @return the environment state at the next tick
   */
  def advance(delta: FiniteDuration): Environment

private final case class EnvironmentImpl(startDateTime: LocalDateTime, time: FiniteDuration) extends Environment:

  override def weather(point: GeographicPoint): WeatherConditions =
    val current = currentDateTime
    val dayOfYear = current.getDayOfYear
    val declination = SolarModel.solarDeclinationDeg(dayOfYear)

    val utcHour = current.getHour + current.getMinute / 60.0 + current.getSecond / 3600.0
    val localHour = SolarModel.localSolarHour(utcHour, point.longitude)

    val (sunrise, sunset) = SolarModel.sunriseSunset(point.latitude, declination)
    val peakToday = SolarModel.clearSkyIrradiance(SolarModel.noonElevationDeg(point.latitude, declination))
    val irradiance = SolarModel.irradianceAt(localHour, sunrise, sunset, peakToday)

    val baseTemperatureC = 15.0
    val seasonalC = SolarModel.seasonalTemperatureOffsetC(point.latitude, declination)
    val dailyC = SolarModel.dailyTemperatureOffsetC(localHour)
    val temperature = Temperature.celsius(baseTemperatureC + seasonalC + dailyC).toAny

    WeatherConditions(irradiance, temperature)

  override def advance(delta: FiniteDuration): Environment =
    copy(time = time + delta)

object Environment:
  /** Reference start date used when no explicit calendar start is given (tests, DSL defaults). */
  private val DefaultStartDateTime: LocalDateTime = LocalDateTime.now()

  /** Backward-compatible: elapsed time only, counted from [[DefaultStartDateTime]]. */
  def apply(time: FiniteDuration): Environment =
    EnvironmentImpl(DefaultStartDateTime, time)

  /** Starts the simulation clock at a specific calendar moment. */
  def apply(startDateTime: LocalDateTime, time: FiniteDuration = 0.seconds): Environment =
    EnvironmentImpl(startDateTime, time)
