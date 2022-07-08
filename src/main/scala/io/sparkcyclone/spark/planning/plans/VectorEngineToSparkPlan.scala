package io.sparkcyclone.spark.planning.plans

import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin._
import io.sparkcyclone.spark.planning.{PlanMetrics, SupportsVeColBatch}
import io.sparkcyclone.util.CallContextOps._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SparkPlan, UnaryExecNode}

/*
  Annotate the class with `ColumnarToRowTransition` to prevent Spark from injecting
  `RowToColumnarExec`s and `ColumnarToRowExec`s into the physical plan.
*/
case class VectorEngineToSparkPlan(override val child: SparkPlan) extends UnaryExecNode
                                                                  with ColumnarToRowTransition
                                                                  with PlanMetrics
                                                                  with LazyLogging {
  override lazy val metrics = invocationMetrics(PLAN) ++ invocationMetrics(BATCH) ++ batchMetrics(INPUT) ++ batchMetrics(OUTPUT)

  /*
    This `SparkPlan` will NOT have `supportsColumnar` be set to `true`, in order
    to force the `doExecute()` to run and generate `RDD[InternalRow]`.  The
    underlying implementation, however, will be calling `executeVeColumnar()` on
    the child plan, which will be a `SupportsVeColBatch` plan.
  */
  override def doExecute: RDD[InternalRow] = {
    require(child.isInstanceOf[SupportsVeColBatch], s"Child plan ${child.getClass.getName} is not a VE plan!")
    initializeMetrics()

    child
      // Cast tthe child plan into SupportsVeColBatch
      .asInstanceOf[SupportsVeColBatch]
      // Call `executeVeColumnar` instead of `executeColumnar`
      .executeVeColumnar
      .mapPartitions { iterator =>
        incrementInvocations(PLAN)

        // For each `VeColBatch`, expand into `Iterator[InternalRow]`
        iterator.flatMap { vbatch =>
          collectBatchMetrics(INPUT, vbatch)

          withInvocationMetrics(BATCH) {
            try {
              // Copy batch back to VH first and iterate
              vbatch.toBytePointerColBatch.internalRowIterator

            } finally {
              // Free the `VeColBatch` on VE side
              child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup(vbatch)
            }
          }
        }
      }
  }

  override def output: Seq[Attribute] = {
    child.output
  }
}
