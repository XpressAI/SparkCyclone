package io.sparkcyclone.spark.plans

import io.sparkcyclone.data.vector.{VeBatchOfBatches, VeColBatch}
import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin.{source, veProcess, vectorEngine}
import io.sparkcyclone.spark.transformation.VeFunction
import io.sparkcyclone.util.CallContext
import io.sparkcyclone.util.CallContextOps._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class VeFlattenPartition(flattenFunction: VeFunction, child: SparkPlan)
  extends UnaryExecNode
  with SupportsVeColBatch
  with LazyLogging
  with PlanMetrics
  with PlanCallsVeFunction {

  require(
    output.size == flattenFunction.outputs.size,
    s"Expected output size ${output.size} to match flatten function results size, but got ${flattenFunction.outputs.size}"
  )

  override lazy val metrics = invocationMetrics(PLAN) ++ invocationMetrics(VE) ++ batchMetrics(INPUT) ++ batchMetrics(OUTPUT)

  override def executeVeColumnar(): RDD[VeColBatch] = {
    initializeMetrics()

    child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
      .mapPartitions { veColBatches =>
        withInvocationMetrics(PLAN){
          withVeLibrary { libRefExchange =>
            collectBatchMetrics(OUTPUT, Iterator
              .continually {
                logger.info(s"About to fetch VeColBatches for flattening a partition...")
                val inputBatches = collectBatchMetrics(INPUT, veColBatches).toList
                //logger.info(s"Fetched all the data: ${inputBatches.toString().take(80)}")
                inputBatches match {
                  case one :: Nil => Iterator(one)
                  case Nil        => Iterator.empty
                  case _ =>
                    Iterator {
                      VeColBatch(try {
                        val res = withInvocationMetrics(VE){
                          vectorEngine.executeMultiIn(
                            libRefExchange,
                            flattenFunction.name,
                            VeBatchOfBatches(inputBatches),
                            flattenFunction.outputs
                          )
                        }
                        logger.info(s"Transformed input, got ${res}")
                        res
                      } finally {
                        inputBatches
                          .foreach(child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup)
                      })
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
