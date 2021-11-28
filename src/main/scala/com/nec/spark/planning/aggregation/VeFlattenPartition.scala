package com.nec.spark.planning.aggregation

import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
import com.nec.spark.planning.OneStageEvaluationPlan.VeFunction
import com.nec.spark.planning.SupportsVeColBatch
import com.nec.ve.VeColBatch
import com.nec.ve.VeColBatch.VeBatchOfBatches
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

import java.nio.file.Paths

case class VeFlattenPartition(flattenFunction: VeFunction, child: SparkPlan)
  extends UnaryExecNode
  with SupportsVeColBatch {
  override def executeVeColumnar(): RDD[VeColBatch] = child
    .asInstanceOf[SupportsVeColBatch]
    .executeVeColumnar()
    .mapPartitions { veColBatches =>
      val libRefExchange = veProcess.loadLibrary(Paths.get(flattenFunction.libraryPath))
      Iterator
        .continually {
          import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
          VeColBatch.fromList(
            veProcess.executeMultiIn(
              libraryReference = libRefExchange,
              functionName = flattenFunction.functionName,
              batches = VeBatchOfBatches.fromVeColBatches(veColBatches.toList),
              results = flattenFunction.results
            )
          )
        }
        .take(1)
    }

  override def output: Seq[Attribute] = ???
}
