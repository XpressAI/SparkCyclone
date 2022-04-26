package com.nec.spark.planning.plans

import com.nec.cache.ArrowEncodingSettings
import com.nec.colvector.{VeBatchOfBatches, VeColBatch}
import com.nec.spark.SparkCycloneExecutorPlugin.{source, veProcess}
import com.nec.spark.planning.{PlanCallsVeFunction, PlanMetrics, SupportsVeColBatch, VeFunction}
import com.nec.ve.VeProcess.OriginalCallingContext
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class VeAmplifyBatchesPlan(amplifyFunction: VeFunction, child: SparkPlan)
  extends UnaryExecNode
  with SupportsVeColBatch
  with LazyLogging
  with PlanMetrics
  with PlanCallsVeFunction {

  require(
    output.size == amplifyFunction.results.size,
    s"Expected output size ${output.size} to match flatten function results size, but got ${amplifyFunction.results.size}"
  )

  override lazy val metrics = invocationMetrics(PLAN) ++ invocationMetrics(VE) ++ batchMetrics(INPUT) ++ batchMetrics(OUTPUT)

  private val encodingSettings = ArrowEncodingSettings.fromConf(conf)(sparkContext)

  override def executeVeColumnar(): RDD[VeColBatch] = {
    initializeMetrics()

    child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
      .mapPartitions { veColBatches =>
        withVeLibrary { libRefExchange =>
          import com.nec.util.BatchAmplifier.Implicits._
          withInvocationMetrics(PLAN){
            collectBatchMetrics(OUTPUT, collectBatchMetrics(INPUT, veColBatches)
              .amplify(limit = encodingSettings.batchSizeTargetBytes, f = _.totalBufferSize)
              .map {
                case inputBatches if inputBatches.size == 1 => inputBatches.head
                case inputBatches =>
                  import OriginalCallingContext.Automatic._
                  try {
                      val res = withInvocationMetrics(VE) {
                        VeColBatch(
                          veProcess.executeMultiIn(
                            libraryReference = libRefExchange,
                            functionName = amplifyFunction.functionName,
                            batches = VeBatchOfBatches(inputBatches.toList),
                            results = amplifyFunction.namedResults
                          )
                        )
                      }
                    logger.debug(
                      s"Transformed input, got ${res}; produced a batch of size ${res.totalBufferSize} bytes"
                    )
                    res
                  } finally {
                    inputBatches.foreach(child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup)
                  }
                })
            }
          }
      }
  }

  override def output: Seq[Attribute] = child.output

  override def veFunction: VeFunction = amplifyFunction

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(amplifyFunction = f(amplifyFunction))
}
