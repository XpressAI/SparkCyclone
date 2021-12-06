package com.nec.spark.planning

import com.nec.spark.planning.SupportsVeColBatch.DataCleanup
import com.nec.ve.VeColBatch
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class VeFetchFromCachePlan(child: SparkPlan) extends UnaryExecNode with SupportsVeColBatch {
  override def executeVeColumnar(): RDD[VeColBatch] = child
    .executeColumnar()
    .map(cb => VeCachedBatchSerializer.unwrapBatch(cb))

  override def output: Seq[Attribute] = child.output

  override def dataCleanup: SupportsVeColBatch.DataCleanup = DataCleanup.NoCleanup
}
