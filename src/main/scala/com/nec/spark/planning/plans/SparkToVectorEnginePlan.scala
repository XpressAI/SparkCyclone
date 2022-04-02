package com.nec.spark.planning.plans

import com.nec.arrow.ArrowEncodingSettings
import com.nec.cache.{ColumnarBatchToVeColBatch, CycloneCacheBase, DualMode}
import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.spark.planning.plans.SparkToVectorEnginePlan.ConvertColumnarToColumnar
import com.nec.spark.planning.{DataCleanup, PlanMetrics, SupportsVeColBatch}
import com.nec.ve.VeColBatch
import com.nec.ve.VeProcess.OriginalCallingContext
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.util.ArrowUtilsExposed

object SparkToVectorEnginePlan {
//  val ConvertColumnarToColumnar = false
  val ConvertColumnarToColumnar = true
}
case class SparkToVectorEnginePlan(childPlan: SparkPlan)
  extends UnaryExecNode
  with LazyLogging
  with SupportsVeColBatch
  with PlanMetrics {

  override lazy val metrics = invocationMetrics(PLAN) ++ invocationMetrics(VE) ++ batchMetrics(INPUT) ++ batchMetrics(OUTPUT)

  override protected def doCanonicalize(): SparkPlan = super.doCanonicalize()

  override def child: SparkPlan = childPlan

  override def output: Seq[Attribute] = child.output

  override def dataCleanup: DataCleanup = DataCleanup.cleanup(this.getClass)

  private def metricsFn[T](f:() => T): T = withInvocationMetrics(VE)(f.apply())

  override def executeVeColumnar(): RDD[VeColBatch] = {
    require(!child.isInstanceOf[SupportsVeColBatch], "Child should not be a VE plan")

    // Instead of creating a new config we are reusing columnBatchSize. In the future if we do
    // combine with some of the Arrow conversion tools we will need to unify some of the configs.
    implicit val arrowEncodingSettings = ArrowEncodingSettings.fromConf(conf)(sparkContext)

    if (child.supportsColumnar) {
      child
        .executeColumnar()
        .mapPartitions { columnarBatches =>
          withInvocationMetrics(PLAN) {
            import SparkCycloneExecutorPlugin._
            implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
              .newChildAllocator(s"Writer for partial collector (ColBatch-->Arrow)", 0, Long.MaxValue)
            TaskContext.get().addTaskCompletionListener[Unit](_ => allocator.close())
            import ImplicitMetrics._
            import OriginalCallingContext.Automatic._
            if (ConvertColumnarToColumnar) {
              collectBatchMetrics(OUTPUT, ColumnarBatchToVeColBatch.toVeColBatchesViaCols(
                columnarBatches = collectBatchMetrics(INPUT, columnarBatches),
                arrowSchema = CycloneCacheBase.makaArrowSchema(child.output),
                completeInSpark = true,
                metricsFn
              ))}
            else
              collectBatchMetrics(OUTPUT, ColumnarBatchToVeColBatch.toVeColBatchesViaRows(
                columnarBatches = collectBatchMetrics(INPUT, columnarBatches),
                arrowSchema = CycloneCacheBase.makaArrowSchema(child.output),
                completeInSpark = true,
                metricsFn
              ))
          }
        }
    } else {
      child.execute().mapPartitions { internalRows =>
        import SparkCycloneExecutorPlugin._
        import ImplicitMetrics._

        withInvocationMetrics(PLAN){
          implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
            .newChildAllocator(s"Writer for partial collector (Arrow)", 0, Long.MaxValue)
          TaskContext.get().addTaskCompletionListener[Unit](_ => allocator.close())
          import OriginalCallingContext.Automatic._

          collectBatchMetrics(OUTPUT, DualMode.unwrapPossiblyDualToVeColBatches(
            possiblyDualModeInternalRows = internalRows,
            arrowSchema = CycloneCacheBase.makaArrowSchema(child.output),
            metricsFn
          ))
        }
      }
    }
  }
}
