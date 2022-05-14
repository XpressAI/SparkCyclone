package com.nec.spark.planning.plans

import com.nec.colvector.VeColBatch
import com.nec.spark.SparkCycloneExecutorPlugin.{source, veProcess, veMetrics}
import com.nec.spark.planning.{PlanCallsVeFunction, PlanMetrics, SupportsVeColBatch, VeFunction}
import com.nec.util.CallContext
import com.nec.ve.VeRDDOps.RichKeyedRDDL
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class VePartialAggregate(
  expectedOutputs: Seq[NamedExpression],
  partialFunction: VeFunction,
  child: SparkPlan
) extends UnaryExecNode
  with SupportsVeColBatch
  with LazyLogging
  with PlanMetrics
  with PlanCallsVeFunction {

  require(
    expectedOutputs.size == partialFunction.results.size,
    s"Expected outputs ${expectedOutputs.size} to match final function results size, but got ${partialFunction.results.size}"
  )

  override lazy val metrics = invocationMetrics(PLAN) ++ invocationMetrics(BATCH) ++ invocationMetrics(VE) ++ batchMetrics(INPUT) ++ batchMetrics(OUTPUT)

  override def executeVeColumnar(): RDD[VeColBatch] = {
    import com.nec.util.CallContextOps._
    initializeMetrics()

    child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
      .mapPartitions { veColBatches =>
        incrementInvocations(PLAN)
        withVeLibrary { libRef =>
          logger.info(s"Will map partial aggregates using $partialFunction")
          collectBatchMetrics(OUTPUT, veColBatches.flatMap { veColBatch =>
            collectBatchMetrics(INPUT, veColBatch)

            withInvocationMetrics(BATCH) {
              import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
              logger.debug(s"Mapping a VeColBatch $veColBatch")

              try {
                val result = withInvocationMetrics(VE) {
                  veMetrics.measureRunningTime(
                    veProcess.executeMulti(
                      libraryReference = libRef,
                      functionName = partialFunction.functionName,
                      cols = veColBatch.columns.toList,
                      results = partialFunction.namedResults
                    )
                  )(
                    veMetrics
                      .registerFunctionCallTime(_, veFunction.functionName)
                  )
                }
                logger.debug(s"Mapped $veColBatch to $result")

                result.flatMap {
                  case (n, l) if l.head.nonEmpty =>
                    Option(n -> VeColBatch(l))
                  case (_, l) =>
                    l.foreach(_.free())
                    None
                }
              } finally {
                child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup(veColBatch)
              }
            }
          })
        }
      }.exchangeBetweenVEs()
  }

  // this is wrong, but just to please spark
  override def output: Seq[Attribute] = expectedOutputs.map(_.toAttribute)

  override def veFunction: VeFunction = partialFunction

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(partialFunction = f(partialFunction))
}
