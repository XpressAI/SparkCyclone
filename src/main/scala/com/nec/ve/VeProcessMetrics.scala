package com.nec.ve

import org.apache.spark.metrics.source.ProcessExecutorMetrics.AllocationTracker

trait VeProcessMetrics {
  def allocationTracker: AllocationTracker
  def getAllocations: Map[Long, Long]
  def measureRunningTime[T](toMeasure: => T)(registerTime: Long => Unit): T
  def checkTotalUsage(): Long
  def registerAllocation(amount: Long, position: Long): Unit
  def deregisterAllocation(position: Long): Unit
  def registerVeCall(timeTaken: Long): Unit
  def registerConversionTime(timeTaken: Long): Unit
  def registerSerializationTime(timeTaken: Long): Unit
  def registerDeserializationTime(timeTaken: Long): Unit
  def registerFunctionCallTime(timeTaken: Long, functionName: String): Unit
}

object VeProcessMetrics {
  def noOp: VeProcessMetrics = NoOp
  private object NoOp extends VeProcessMetrics {
    override def registerAllocation(amount: Long, position: Long): Unit = ()
    override def deregisterAllocation(position: Long): Unit = ()
    override def registerConversionTime(timeTaken: Long): Unit = ()
    override def registerVeCall(timeTaken: Long): Unit = ()
    override def registerFunctionCallTime(timeTaken: Long, functionName: String): Unit = ()
    override def registerSerializationTime(timeTaken: Long): Unit = ()
    override def registerDeserializationTime(timeTaken: Long): Unit = ()
    override def checkTotalUsage(): Long = Long.MinValue
    override def measureRunningTime[T](toMeasure: => T)(registerTime: Long => Unit): T = toMeasure
    override def allocationTracker: AllocationTracker = AllocationTracker.noOp
    override def getAllocations: Map[Long, Long] = Map.empty
  }
}
