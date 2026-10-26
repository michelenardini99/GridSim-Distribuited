package org.gridsim.core.behaviour.house

import org.gridsim.core.behaviour.house.ConsumptionBand

/**
 * Encapsulates the demand behavior of a house based on its occupancy or usage profile.
 *
 * A strategy provides the [[ConsumptionBand]] (statistical parameters) for each hour,
 * allowing the execution engine to handle stochasticity and energy conversions.
 */
trait ConsumptionStrategy extends java.io.Serializable:
  /**
   * Provides the statistical consumption parameters for a given hour.
   *
   * @param h The hour of the day (0-23).
   * @return The [[ConsumptionBand]] defining mean power and variance for that hour.
   */
  def getBand(h: Long): ConsumptionBand
