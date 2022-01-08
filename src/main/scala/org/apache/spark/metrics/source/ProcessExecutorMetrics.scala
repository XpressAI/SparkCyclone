package org.apache.spark.metrics.source

import com.codahale.metrics.{Gauge, MetricRegistry}
import com.nec.ve.VeProcessMetrics
import org.apache.spark.metrics.source.ProcessExecutorMetrics.AllocationTracker
import org.apache.spark.metrics.source.ProcessExecutorMetrics.AllocationTracker.Allocation

import scala.collection.mutable

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

final class ProcessExecutorMetrics(val allocationTracker: AllocationTracker)
  extends VeProcessMetrics
  with Source {
  private val allocations: scala.collection.mutable.Map[Long, Long] = mutable.Map.empty

  override def registerAllocation(amount: Long, position: Long): Unit = {
    allocations.put(position, amount)
    allocationTracker.track(position)
  }

  override def deregisterAllocation(position: Long): Unit = {
    allocations.remove(position)
    allocationTracker.untrack(position)
  }

  override def sourceName: String = "VEProcessExecutor"

  override val metricRegistry: MetricRegistry = new MetricRegistry()

  metricRegistry.register(
    MetricRegistry.name("ve", "allocations"),
    new Gauge[Long] {
      override def getValue: Long = allocations.size
    }
  )

  metricRegistry.register(
    MetricRegistry.name("ve", "bytesAllocated"),
    new Gauge[Long] {
      override def getValue: Long = allocations.valuesIterator.sum
    }
  )

  def getAllocations: Map[Long, Long] = allocations.toMap

}
