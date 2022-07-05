package io.sparkcyclone.spark.planning.aggregation

import io.sparkcyclone.colvector.{VeBatchOfBatches, VeColBatch}
import io.sparkcyclone.spark.SparkCycloneExecutorPlugin.{source, veProcess, vectorEngine}
import io.sparkcyclone.spark.planning.{PlanCallsVeFunction, PlanMetrics, SupportsVeColBatch, VeFunction}
import io.sparkcyclone.util.CallContext
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class VeFlattenPartition(flattenFunction: VeFunction, child: SparkPlan)
  extends UnaryExecNode
  with SupportsVeColBatch
  with Logging
  with PlanCallsVeFunction
  with PlanMetrics
  with LazyLogging {

  require(
    output.size == flattenFunction.outputs.size,
    s"Expected output size ${output.size} to match flatten function results size, but got ${flattenFunction.outputs.size}"
  )

  override lazy val metrics = invocationMetrics(PLAN) ++ invocationMetrics(BATCH) ++ invocationMetrics(VE) ++ batchMetrics(INPUT) ++ batchMetrics(OUTPUT)

  override def executeVeColumnar(): RDD[VeColBatch] = {
    initializeMetrics()

    child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
      .mapPartitions { veColBatches =>
        withVeLibrary { libRefExchange =>
          withInvocationMetrics(PLAN){
            collectBatchMetrics(OUTPUT, Iterator
              .continually {
                val inputBatches = collectBatchMetrics(INPUT, veColBatches).toList
                //logger.debug(s"Fetched all the data: ${inputBatches}")
                inputBatches match {
                  case one :: Nil => withInvocationMetrics(BATCH){ Iterator(one) }
                  case Nil        => Iterator.empty
                  case _ =>
                    Iterator {
                      import io.sparkcyclone.util.CallContextOps._

                      withInvocationMetrics(VE){
                        VeColBatch(
                          try {
                            vectorEngine.executeMultiIn(
                              libRefExchange,
                              flattenFunction.name,
                              VeBatchOfBatches(inputBatches),
                              flattenFunction.outputs
                            )
                          } finally {
                            logger.debug("Transformed input.")
                            inputBatches
                              .foreach(child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup)
                          }
                        )
                      }
                    }
                }
              }
              .take(1)
              .flatten)
          }
        }
      }
  }

  override def output: Seq[Attribute] = child.output

  override def veFunction: VeFunction = flattenFunction

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(flattenFunction = f(flattenFunction))
}
