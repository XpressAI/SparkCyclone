package io.sparkcyclone.metrics

import com.codahale.metrics._

final class ProcessExecutorMetrics(registry: MetricRegistry) extends VeProcessMetrics {
  private var arrowConversionTime: Long = 0L
  private val arrowConversionHist = new Histogram(new UniformReservoir())
  private val serializationHist = new Histogram(new UniformReservoir())
  private val deserializationHist = new Histogram(new UniformReservoir())

  override def measureRunningTime[T](toMeasure: => T)(registerTime: Long => Unit): T = {
    val start = System.currentTimeMillis()
    val result = toMeasure
    val end = System.currentTimeMillis()

    registerTime(end - start)
    result
  }

  override def registerConversionTime(timeTaken: Long): Unit = {
    arrowConversionTime += timeTaken
    arrowConversionHist.update(timeTaken)
  }

  override def registerSerializationTime(timeTaken: Long): Unit = {
    serializationHist.update(timeTaken)
  }

  override def registerDeserializationTime(timeTaken: Long): Unit = {
    deserializationHist.update(timeTaken)
  }

  registry.register(
    MetricRegistry.name("ve", "arrowConversionTime"),
    new Gauge[Long] {
      override def getValue: Long = arrowConversionTime
    }
  )

  registry.register(MetricRegistry.name("ve", "arrowConversionTimeHist"), arrowConversionHist)
  registry.register(MetricRegistry.name("ve", "serializationTime"), serializationHist)
  registry.register(MetricRegistry.name("ve", "deserializationTime"), deserializationHist)
}
