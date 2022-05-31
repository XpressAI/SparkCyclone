package org.apache.spark.metrics.source

import com.codahale.metrics.{Gauge, Histogram, MetricRegistry, UniformReservoir}
import com.nec.ve.VeProcessMetrics
import org.apache.spark.metrics.source.ProcessExecutorMetrics.AllocationTracker
import org.apache.spark.metrics.source.ProcessExecutorMetrics.AllocationTracker.Allocation

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ProcessExecutorMetrics {
  trait AllocationTracker {
    def track(position: Long): Unit
    def untrack(position: Long): Unit
    def remaining: List[Allocation]
  }
  object AllocationTracker {
    final case class Allocation(position: Long, stackTrace: List[StackTraceElement])
    def noOp: AllocationTracker = NoOpTracker
    def simple(): AllocationTracker = new CapturingTracker()
    private object NoOpTracker extends AllocationTracker {
      def track(position: Long): Unit = ()
      val remaining: List[Allocation] = Nil
      def untrack(position: Long): Unit = ()
    }
    private final class CapturingTracker extends AllocationTracker {
      private val allocations = scala.collection.mutable.Map.empty[Long, Allocation]
      override def track(position: Long): Unit =
        allocations.put(
          position,
          Allocation(position = position, stackTrace = new Exception().getStackTrace.toList)
        )
      override def remaining: List[Allocation] =
        allocations.valuesIterator.toList
      override def untrack(position: Long): Unit = allocations.remove(position)
    }
  }
}

final class ProcessExecutorMetrics(val allocationTracker: AllocationTracker, registry: MetricRegistry)
  extends VeProcessMetrics
  with Source {
  private val allocations: scala.collection.mutable.Map[Long, Long] = mutable.Map.empty
  private val veCalls: ArrayBuffer[Long] = new ArrayBuffer[Long]()
  private var totalTransferTime: Long = 0L
  private var arrowConversionTime: Long = 0L
  private val arrowConversionHist = new Histogram(new UniformReservoir())
  private val serializationHist = new Histogram(new UniformReservoir())
  private val deserializationHist = new Histogram(new UniformReservoir())
  private val perFunctionHistograms: scala.collection.mutable.Map[String, Histogram] =
    mutable.Map.empty

  override def measureRunningTime[T](toMeasure: => T)(registerTime: Long => Unit): T = {
    val start = System.currentTimeMillis()
    val result = toMeasure
    val end = System.currentTimeMillis()

    registerTime(end - start)
    result
  }

  override def registerAllocation(amount: Long, position: Long): Unit = {
    allocations.put(position, amount)
    allocationTracker.track(position)
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

  override def deregisterAllocation(position: Long): Unit = {
    allocations.remove(position)
    allocationTracker.untrack(position)
  }

  override def registerFunctionCallTime(timeTaken: Long, functionName: String): Unit = {
    perFunctionHistograms.get(functionName) match {
      case Some(hist) => hist.update(timeTaken)
      case None => {
        val hist = new Histogram(new UniformReservoir())
        try {
          metricRegistry.register(MetricRegistry.name("ve", s"veCallTimeHist_${functionName}"), hist)
          perFunctionHistograms.put(functionName, hist)
          hist.update(timeTaken)
        }catch{
          case e: Exception => // NoOP
        }
      }
    }
  }

  override def registerVeCall(timeTaken: Long): Unit = veCalls.append(timeTaken)

  override def sourceName: String = "VEProcessExecutor"

  override val metricRegistry: MetricRegistry = registry

  metricRegistry.register(
    MetricRegistry.name("ve", "allocations"),
    new Gauge[Long] {
      override def getValue: Long = allocations.size
    }
  )

  metricRegistry.register(
    MetricRegistry.name("ve", "arrowConversionTime"),
    new Gauge[Long] {
      override def getValue: Long = arrowConversionTime
    }
  )

  metricRegistry.register(MetricRegistry.name("ve", "arrowConversionTimeHist"), arrowConversionHist)
  metricRegistry.register(MetricRegistry.name("ve", "serializationTime"), serializationHist)
  metricRegistry.register(MetricRegistry.name("ve", "deserializationTime"), deserializationHist)

  metricRegistry.register(
    MetricRegistry.name("ve", "transferTime"),
    new Gauge[Long] {
      override def getValue: Long = totalTransferTime
    }
  )

  metricRegistry.register(
    MetricRegistry.name("ve", "callTime"),
    new Gauge[Long] {
      override def getValue: Long = veCalls.sum
    }
  )

  metricRegistry.register(
    MetricRegistry.name("ve", "calls"),
    new Gauge[Long] {
      override def getValue: Long = veCalls.size
    }
  )

  metricRegistry.register(
    MetricRegistry.name("ve", "bytesAllocated"),
    new Gauge[Long] {
      override def getValue: Long = allocations.valuesIterator.sum
    }
  )

  def getAllocations: Map[Long, Long] = allocations.toMap

  override def checkTotalUsage(): Long = allocations.valuesIterator.sum

}
