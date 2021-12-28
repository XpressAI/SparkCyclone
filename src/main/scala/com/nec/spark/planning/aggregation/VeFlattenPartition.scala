package com.nec.spark.planning.aggregation

import com.nec.spark.SparkCycloneExecutorPlugin.{source, veProcess}
import com.nec.spark.planning.{PlanCallsVeFunction, SupportsVeColBatch, VeFunction}
import com.nec.ve.VeColBatch
import com.nec.ve.VeColBatch.VeBatchOfBatches
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class VeFlattenPartition(flattenFunction: VeFunction, child: SparkPlan)
  extends UnaryExecNode
  with SupportsVeColBatch
  with Logging
  with PlanCallsVeFunction {

  require(
    output.size == flattenFunction.results.size,
    s"Expected output size ${output.size} to match flatten function results size, but got ${flattenFunction.results.size}"
  )

  override def executeVeColumnar(): RDD[VeColBatch] = child
    .asInstanceOf[SupportsVeColBatch]
    .executeVeColumnar()
    .mapPartitions { veColBatches =>
      withVeLibrary { libRefExchange =>
        Iterator
          .continually {
            import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
            val inputBatches = veColBatches.toList
            logInfo(s"Fetched all the data: ${inputBatches}")
            inputBatches match {
              case one :: Nil => Iterator(one)
              case Nil        => Iterator.empty
              case _ =>
                Iterator {
                  VeColBatch.fromList(
                    try veProcess.executeMultiIn(
                      libraryReference = libRefExchange,
                      functionName = flattenFunction.functionName,
                      batches = VeBatchOfBatches.fromVeColBatches(inputBatches),
                      results = flattenFunction.results
                    )
                    finally {
                      logInfo("Transformed input.")
                      inputBatches
                        .foreach(child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup)
                    }
                  )
                }
            }
          }
          .take(1)
          .flatten
      }
    }

  override def output: Seq[Attribute] = child.output

  override def veFunction: VeFunction = flattenFunction

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(flattenFunction = f(flattenFunction))
}
