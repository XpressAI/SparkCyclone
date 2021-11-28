package com.nec.spark.planning.aggregation

import com.nec.spark.planning.OneStageEvaluationPlan.VeFunction
import com.nec.spark.planning.SupportsVeColBatch
import com.nec.ve.VeColBatch
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

import java.nio.file.Paths

case class VeFinalAggregate(
  expectedOutputs: Seq[NamedExpression],
  finalFunction: VeFunction,
  child: SparkPlan
) extends UnaryExecNode
  with SupportsVeColBatch
  with Logging {
  import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
  override def executeVeColumnar(): RDD[VeColBatch] = child
    .asInstanceOf[SupportsVeColBatch]
    .executeVeColumnar()
    .mapPartitions { veColBatches =>
      val libRef = veProcess.loadLibrary(Paths.get(finalFunction.libraryPath))
      veColBatches.map { veColBatch =>
        logInfo("Preparing to final-aggregate a batch...")

        import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
        VeColBatch.fromList {
          try veProcess.execute(
            libraryReference = libRef,
            functionName = finalFunction.functionName,
            cols = veColBatch.cols,
            results = finalFunction.results
          )
          finally {
            logInfo("Completed a final-aggregate of  a batch...")
            veColBatch.cols.foreach(_.free())
          }
        }
      }
    }

  override def output: Seq[Attribute] = expectedOutputs.map(_.toAttribute)
}
