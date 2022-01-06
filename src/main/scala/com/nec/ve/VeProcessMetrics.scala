package com.nec.ve

trait VeProcessMetrics {
  def registerAllocation(position: Long, amount: Long): Unit
  def deregisterAllocation(position: Long): Unit
}

object VeProcessMetrics {
  object NoOp extends VeProcessMetrics {
    override def registerAllocation(position: Long, amount: Long): Unit = ()
    override def deregisterAllocation(position: Long): Unit = ()
  }
}
