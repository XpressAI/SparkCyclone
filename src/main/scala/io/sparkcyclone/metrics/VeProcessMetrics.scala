package io.sparkcyclone.metrics

trait VeProcessMetrics {
  def measureRunningTime[T](toMeasure: => T)(registerTime: Long => Unit): T
  def registerConversionTime(timeTaken: Long): Unit
  def registerSerializationTime(timeTaken: Long): Unit
  def registerDeserializationTime(timeTaken: Long): Unit
}

object VeProcessMetrics {
  def noOp: VeProcessMetrics = NoOp
  private object NoOp extends VeProcessMetrics {
    override def registerConversionTime(timeTaken: Long): Unit = ()
    override def registerSerializationTime(timeTaken: Long): Unit = ()
    override def registerDeserializationTime(timeTaken: Long): Unit = ()
    override def measureRunningTime[T](toMeasure: => T)(registerTime: Long => Unit): T = toMeasure
  }
}
