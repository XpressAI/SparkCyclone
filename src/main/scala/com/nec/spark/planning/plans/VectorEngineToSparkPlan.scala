package com.nec.spark.planning.plans

import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.spark.planning.ArrowBatchToUnsafeRows.mapBatchToRow
import com.nec.spark.planning.{PlanMetrics, SupportsVeColBatch}
import com.nec.util.CallContext
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.ColumnarBatch

case class VectorEngineToSparkPlan(override val child: SparkPlan)
  extends UnaryExecNode
  with PlanMetrics
  with LazyLogging {
  override def supportsColumnar: Boolean = true

  override lazy val metrics = invocationMetrics(PLAN) ++ invocationMetrics(BATCH) ++ batchMetrics(INPUT) ++ batchMetrics(OUTPUT)


  override def doExecute(): RDD[InternalRow] = {
    initializeMetrics()

    doExecuteColumnar().mapPartitions(columnarBatchIterator => {
      columnarBatchIterator.flatMap(mapBatchToRow)
    })
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    initializeMetrics()

    child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
      .mapPartitions { iterator =>
        incrementInvocations(PLAN)

        import SparkCycloneExecutorPlugin._

        lazy implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
          .newChildAllocator(s"Writer for partial collector", 0, Long.MaxValue)

        iterator.map { veColBatch =>
          import com.nec.util.CallContextOps._
          collectBatchMetrics(INPUT, veColBatch)
          withInvocationMetrics(BATCH){
            try {
              logger.debug(s"Mapping veColBatch ${veColBatch} to arrow...")
              val res = veColBatch.toArrowColumnarBatch
              logger.debug(s"Finished mapping ${veColBatch}")
              collectBatchMetrics(OUTPUT, res)
            } finally child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup(veColBatch)
          }
        }
      }
  }

  override def output: Seq[Attribute] = child.output

}
