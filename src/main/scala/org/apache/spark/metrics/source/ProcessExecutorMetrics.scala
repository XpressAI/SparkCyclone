package org.apache.spark.metrics.source

import com.codahale.metrics.{Gauge, Histogram, MetricRegistry, UniformReservoir}
import com.nec.ve.VeProcessMetrics
import scala.collection.mutable.{Map => MMap}

final class ProcessExecutorMetrics(registry: MetricRegistry) extends VeProcessMetrics with Source {
  private var arrowConversionTime: Long = 0L
  private val arrowConversionHist = new Histogram(new UniformReservoir())
  private val serializationHist = new Histogram(new UniformReservoir())
  private val deserializationHist = new Histogram(new UniformReservoir())
  private val perFunctionHistograms: scala.collection.mutable.Map[String, Histogram] = MMap.empty

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

  override def sourceName: String = "VEProcessExecutor"

  override val metricRegistry: MetricRegistry = registry

  metricRegistry.register(
    MetricRegistry.name("ve", "arrowConversionTime"),
    new Gauge[Long] {
      override def getValue: Long = arrowConversionTime
    }
  )

  metricRegistry.register(MetricRegistry.name("ve", "arrowConversionTimeHist"), arrowConversionHist)
  metricRegistry.register(MetricRegistry.name("ve", "serializationTime"), serializationHist)
  metricRegistry.register(MetricRegistry.name("ve", "deserializationTime"), deserializationHist)
}
