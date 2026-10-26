package org.gridsim.core.common

/**
 * Geographic position of a simulation component.
 *
 * Coordinates are expressed in decimal degrees using the WGS84 coordinate
 * system. Latitude ranges from -90° (South Pole) to +90° (North Pole),
 * while longitude ranges from -180° to +180°.
 */
trait GeographicPoint extends java.io.Serializable:
  def latitude: Double
  def longitude: Double

private final case class GeographicPointImpl(latitude: Double, longitude: Double)
  extends GeographicPoint

object GeographicPoint:
  def apply(latitude: Double, longitude: Double): GeographicPoint =
    require(latitude >= -90.0 && latitude <= 90.0, s"Invalid latitude: $latitude")
    require(longitude >= -180.0 && longitude <= 180.0, s"Invalid longitude: $longitude")
    GeographicPointImpl(latitude, longitude)
