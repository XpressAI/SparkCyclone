package org.apache.spark.metrics.source

import com.codahale.metrics.{Gauge, MetricRegistry}
import com.nec.ve.VeProcessMetrics

import scala.collection.mutable

final class ProcessExecutorMetrics() extends VeProcessMetrics with Source {
  private val allocations: scala.collection.mutable.Map[Long, Long] = mutable.Map.empty

  override def registerAllocation(amount: Long, position: Long): Unit =
    allocations.put(position, amount)

  override def deregisterAllocation(position: Long): Unit =
    allocations.remove(position)

  override def sourceName: String = "VE Process Executor Metrics"

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

}
