package com.nec.spark.planning.plans

import com.nec.spark.planning.SupportsVeColBatch.DataCleanup
import com.nec.spark.planning.{SupportsVeColBatch, VeCachedBatchSerializer}
import com.nec.ve.VeColBatch
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class VeFetchFromCachePlan(child: SparkPlan)
  extends UnaryExecNode
  with SupportsVeColBatch
  with LazyLogging {
  override def executeVeColumnar(): RDD[VeColBatch] = child
    .executeColumnar()
    .map(cb => {
      logger.debug(s"Mapping ColumnarBatch ${cb} to VE")
      import com.nec.spark.SparkCycloneExecutorPlugin._
      val veColBatch = VeCachedBatchSerializer.unwrapIntoVE(cb)
      // todo register for deletion
      logger.debug(s"Finished mapping ColumnarBatch ${cb} to VE: ${veColBatch}")

      TaskContext.get().addTaskCompletionListener[Unit] { _ =>
        import com.nec.spark.SparkCycloneExecutorPlugin._
        veColBatch.cols.foreach(_.free())
      }

      veColBatch
    })

  override def output: Seq[Attribute] = child.output

  override def dataCleanup: SupportsVeColBatch.DataCleanup = DataCleanup.Cleanup
}
