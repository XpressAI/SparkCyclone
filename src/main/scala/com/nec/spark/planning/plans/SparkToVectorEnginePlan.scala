package com.nec.spark.planning.plans

import com.nec.spark.planning.SupportsVeColBatch
import com.nec.spark.planning.VeColBatchConverters.{
  getNumRows,
  internalRowToSerializedBatch,
  ByteArrayOrBufferColBatch
}
import com.nec.ve.VeColBatch
import com.nec.ve.VeColBatch.VectorEngineLocation
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class SparkToVectorEnginePlan(childPlan: SparkPlan)
  extends UnaryExecNode
  with LazyLogging
  with SupportsVeColBatch {

  override protected def doCanonicalize(): SparkPlan = {
    super.doCanonicalize()
  }

  override def child: SparkPlan = {
    childPlan
  }
  override def executeVeColumnar(): RDD[VeColBatch] = {
    require(!child.isInstanceOf[SupportsVeColBatch], "Child should not be a VE plan")

    //      val numInputRows = longMetric("numInputRows")
    //      val numOutputBatches = longMetric("numOutputBatches")
    // Instead of creating a new config we are reusing columnBatchSize. In the future if we do
    // combine with some of the Arrow conversion tools we will need to unify some of the configs.
    val numRows: Int = getNumRows(sparkContext, conf)
    logger.info(s"Will make batches of ${numRows} rows...")
    val timeZoneId = conf.sessionLocalTimeZone
    internalRowToSerializedBatch(child.execute(), timeZoneId, child.schema, numRows)
      .map { bb =>
        val result = bb match {
          case ByteArrayOrBufferColBatch(Left(colsByteBuffer)) =>
            import com.nec.spark.SparkCycloneExecutorPlugin._
            import com.nec.ve.ByteBufferVeColVector._
            VeColBatch.fromList(
              colsByteBuffer.map(
                _.transferBuffersToVe()
                  .map(_.getOrElse(VectorEngineLocation(-1)))
                  .newContainer()
              )
            )
          case ByteArrayOrBufferColBatch(Right(colsByteArray)) =>
            import com.nec.spark.SparkCycloneExecutorPlugin._
            import com.nec.ve.ByteArrayColVector._
            VeColBatch.fromList(
              colsByteArray.map(
                _.transferBuffersToVe()
                  .map(_.getOrElse(VectorEngineLocation(-1)))
                  .newContainer()
              )
            )
        }

        TaskContext.get().addTaskCompletionListener[Unit] { _ =>
          import com.nec.spark.SparkCycloneExecutorPlugin._
          result.cols.foreach(_.free())
        }

        result
      }
  }

  override def output: Seq[Attribute] = child.output
}
