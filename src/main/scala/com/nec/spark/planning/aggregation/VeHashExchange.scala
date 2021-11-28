package com.nec.spark.planning.aggregation

import com.nec.spark.planning.OneStageEvaluationPlan.VeFunction
import com.nec.spark.planning.SupportsVeColBatch
import com.nec.ve.VeColBatch
import com.nec.ve.VeRDD.RichKeyedRDDL
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

import java.nio.file.Paths

case class VeHashExchange(exchangeFunction: VeFunction, child: SparkPlan)
  extends UnaryExecNode
  with SupportsVeColBatch {

  import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
  override def executeVeColumnar(): RDD[VeColBatch] = child
    .asInstanceOf[SupportsVeColBatch]
    .executeVeColumnar()
    .mapPartitions { veColBatches =>
      val libRefExchange = veProcess.loadLibrary(Paths.get(exchangeFunction.libraryPath))
      veColBatches.flatMap { veColBatch =>
        import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
        veProcess.executeMulti(
          libraryReference = libRefExchange,
          functionName = exchangeFunction.functionName,
          cols = veColBatch.cols,
          results = exchangeFunction.results
        )
      }
    }
    .exchangeBetweenVEs()
    .mapPartitions(f = _.map(lv => VeColBatch.fromList(lv)), preservesPartitioning = true)

  override def output: Seq[Attribute] = ???
}
