package com.nec.spark.planning.plans

import com.nec.arrow.ArrowEncodingSettings
import com.nec.cache.{ColumnarBatchToVeColBatch, CycloneCacheBase, DualMode}
import com.nec.spark.{SparkCycloneExecutorPlugin, planning}
import com.nec.spark.planning.plans.SparkToVectorEnginePlan.ConvertColumnarToColumnar
import com.nec.spark.planning.{DataCleanup, PlanMetrics, SupportsVeColBatch}
import com.nec.ve.VeColBatch
import com.nec.ve.VeProcess.OriginalCallingContext
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.util.ArrowUtilsExposed

import scala.concurrent.duration.NANOSECONDS

object SparkToVectorEnginePlan {
//  val ConvertColumnarToColumnar = false
  val ConvertColumnarToColumnar = true
}
case class SparkToVectorEnginePlan(childPlan: SparkPlan)
  extends UnaryExecNode
  with LazyLogging
  with SupportsVeColBatch
  with PlanMetrics {

  override lazy val metrics = invocationMetrics(PLAN) ++ invocationMetrics(VE) ++ Map(
    "inputPartitions" -> SQLMetrics.createMetric(sparkContext, "input partitions count"),
    "inputBatchRows" -> SQLMetrics.createAverageMetric(sparkContext, "input batch row count"),
    "inputBatchCols" -> SQLMetrics.createAverageMetric(sparkContext, "input batch column count"),
    "outputBatchRows" -> SQLMetrics.createAverageMetric(sparkContext, "output batch row count"),
    "outputBatchCols" -> SQLMetrics.createAverageMetric(sparkContext, "output batch column count")
  )

  override protected def doCanonicalize(): SparkPlan = super.doCanonicalize()

  override def child: SparkPlan = childPlan

  override def output: Seq[Attribute] = child.output

  override def dataCleanup: DataCleanup = DataCleanup.cleanup(this.getClass)

  override def executeVeColumnar(): RDD[VeColBatch] = {
    require(!child.isInstanceOf[SupportsVeColBatch], "Child should not be a VE plan")

    val inputPartCount = longMetric("inputPartitions")
    val inputBatchRows = longMetric("inputBatchRows")
    val inputBatchCols = longMetric("inputBatchCols")
    val outputBatchRows = longMetric("outputBatchRows")
    val outputBatchCols = longMetric("outputBatchCols")

    //      val numInputRows = longMetric("numInputRows")
    //      val numOutputBatches = longMetric("numOutputBatches")
    // Instead of creating a new config we are reusing columnBatchSize. In the future if we do
    // combine with some of the Arrow conversion tools we will need to unify some of the configs.
    implicit val arrowEncodingSettings = ArrowEncodingSettings.fromConf(conf)(sparkContext)

    if (child.supportsColumnar) {
      val input = child
        .executeColumnar()
      inputPartCount.set(input.partitions.length)

      input
        .mapPartitions { columnarBatches =>
          withInvocationMetrics(PLAN) {
            import SparkCycloneExecutorPlugin._
            implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
              .newChildAllocator(s"Writer for partial collector (ColBatch-->Arrow)", 0, Long.MaxValue)
            TaskContext.get().addTaskCompletionListener[Unit](_ => allocator.close())
            import OriginalCallingContext.Automatic._
            import ImplicitMetrics._
            if (ConvertColumnarToColumnar)
              ColumnarBatchToVeColBatch.toVeColBatchesViaCols(
                columnarBatches = columnarBatches,
                arrowSchema = CycloneCacheBase.makaArrowSchema(child.output),
                completeInSpark = true,
                inputBatchRows = inputBatchRows,
                inputBatchCols = inputBatchCols,
                outputBatchRows = outputBatchRows,
                outputBatchCols = outputBatchCols
              )
            else
              ColumnarBatchToVeColBatch.toVeColBatchesViaRows(
                columnarBatches = columnarBatches,
                arrowSchema = CycloneCacheBase.makaArrowSchema(child.output),
                completeInSpark = true,
                inputBatchRows = inputBatchRows,
                inputBatchCols = inputBatchCols,
                outputBatchRows = outputBatchRows,
                outputBatchCols = outputBatchCols
              )
          }
        }
    } else {
      val input = child.execute()
      inputPartCount.set(input.partitions.length)

      input.mapPartitions { internalRows =>
        import SparkCycloneExecutorPlugin._
        import ImplicitMetrics._

        withInvocationMetrics(PLAN){
          implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
            .newChildAllocator(s"Writer for partial collector (Arrow)", 0, Long.MaxValue)
          TaskContext.get().addTaskCompletionListener[Unit](_ => allocator.close())
          import OriginalCallingContext.Automatic._

          DualMode.unwrapPossiblyDualToVeColBatches(
            possiblyDualModeInternalRows = internalRows,
            arrowSchema = CycloneCacheBase.makaArrowSchema(child.output)
          )
        }
      }
    }
  }
}
