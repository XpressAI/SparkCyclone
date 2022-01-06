package org.apache.spark.metrics.source

import com.codahale.metrics.{Gauge, MetricRegistry}
import com.nec.ve.VeProcessMetrics
import org.apache.spark.metrics.source.DeepProcessExecutorMetrics.{
  AllocationInfo,
  AllocationPosition
}

import scala.collection.mutable

object DeepProcessExecutorMetrics {
  final case class AllocationPosition(value: Long)
  final case class AllocationInfo(size: Long, trace: List[StackTraceElement])
}

final class DeepProcessExecutorMetrics() extends VeProcessMetrics with Source {
  private val allocations: scala.collection.mutable.Map[AllocationPosition, AllocationInfo] =
    mutable.Map.empty

  override def registerAllocation(position: Long, amount: Long): Unit =
    allocations.put(
      AllocationPosition(position),
      AllocationInfo(size = amount, trace = new Exception().getStackTrace.toList)
    )

  override def deregisterAllocation(position: Long): Unit =
    allocations.remove(AllocationPosition(position))

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
      override def getValue: Long = allocations.valuesIterator.map(_.size).sum
    }
  )

  def getAllocations: Map[AllocationPosition, AllocationInfo] = allocations.toMap

}
