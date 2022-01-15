package com.nec.ve

trait VeProcessMetrics {
  def registerAllocation(amount: Long, position: Long): Unit
  def deregisterAllocation(position: Long): Unit
}

object VeProcessMetrics {
  object NoOp extends VeProcessMetrics {
    override def registerAllocation(amount: Long, position: Long): Unit = ()
    override def deregisterAllocation(position: Long): Unit = ()
  }
}
