package io.sparkcyclone.spark.planning.plans

import io.sparkcyclone.cache.ArrowEncodingSettings
import io.sparkcyclone.data.vector.{VeBatchOfBatches, VeColBatch}
import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin.{source, veProcess, vectorEngine}
import io.sparkcyclone.spark.planning.{PlanCallsVeFunction, PlanMetrics, SupportsVeColBatch, VeFunction}
import io.sparkcyclone.util.CallContext
import io.sparkcyclone.util.CallContextOps._
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
    output.size == amplifyFunction.outputs.size,
    s"Expected output size ${output.size} to match flatten function results size, but got ${amplifyFunction.outputs.size}"
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
          import io.sparkcyclone.util.BatchAmplifier.Implicits._
          withInvocationMetrics(PLAN){
            collectBatchMetrics(OUTPUT, collectBatchMetrics(INPUT, veColBatches)
              .amplify(limit = encodingSettings.batchSizeTargetBytes, f = _.totalBufferSize)
              .map {
                case inputBatches if inputBatches.size == 1 => inputBatches.head
                case inputBatches =>
                  try {
                      val res = withInvocationMetrics(VE) {
                        VeColBatch(
                          vectorEngine.executeMultiIn(
                            libRefExchange,
                            amplifyFunction.name,
                            VeBatchOfBatches(inputBatches.toList),
                            amplifyFunction.outputs
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
