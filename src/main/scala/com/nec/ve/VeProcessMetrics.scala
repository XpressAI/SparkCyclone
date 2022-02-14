package com.nec.ve

trait VeProcessMetrics {
  def checkTotalUsage(): Long
  def registerAllocation(amount: Long, position: Long): Unit
  def deregisterAllocation(position: Long): Unit
  def registerVeCall(timeTaken: Long): Unit
  def registerConversionTime(timeTaken: Long): Unit
  def registerTransferTime(timeTaken: Long): Unit
  def registerSerializationTime(timeTaken: Long): Unit
  def registerDeserializationTime(timeTaken: Long): Unit
  def registerFunctionCallTime(timeTaken: Long, functionName: String): Unit

}

object VeProcessMetrics {
  object NoOp extends VeProcessMetrics {
    override def registerAllocation(amount: Long, position: Long): Unit = ()
    override def deregisterAllocation(position: Long): Unit = ()
    override def registerConversionTime(timeTaken: Long): Unit = ()
    override def registerTransferTime(timeTaken: Long): Unit = ()
    override def registerVeCall(timeTaken: Long): Unit = ()
    override def registerFunctionCallTime(timeTaken: Long, functionName: String): Unit = ()
    override def registerSerializationTime(timeTaken: Long): Unit = ()
    override def registerDeserializationTime(timeTaken: Long): Unit = ()
    override def checkTotalUsage(): Long = Long.MinValue
  }
}
