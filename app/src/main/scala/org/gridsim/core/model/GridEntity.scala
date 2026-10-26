package org.gridsim.core.model

/** Base trait for everything that has an identity in the grid. */
trait GridEntity extends java.io.Serializable:
  def id: String

trait GridEntityState extends java.io.Serializable:
  def entityId: String
