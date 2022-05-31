package com.nec.spark.planning.plans

import com.nec.colvector.{VeBatchOfBatches, VeColBatch}
import com.nec.spark.SparkCycloneExecutorPlugin.{source, veProcess, vectorEngine}
import com.nec.spark.planning.{PlanCallsVeFunction, PlanMetrics, SupportsVeColBatch, VeFunction}
import com.nec.util.CallContext
import com.nec.util.CallContextOps._
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
    output.size == flattenFunction.results.size,
    s"Expected output size ${output.size} to match flatten function results size, but got ${flattenFunction.results.size}"
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
                            flattenFunction.functionName,
                            VeBatchOfBatches(inputBatches),
                            flattenFunction.namedResults
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
