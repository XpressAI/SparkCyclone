/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.nec.spark.planning.plans

import com.nec.colvector.VeColBatch
import com.nec.util.CallContextOps._
import com.nec.spark.SparkCycloneExecutorPlugin.{source, veProcess, vectorEngine}
import com.nec.spark.planning.{PlanCallsVeFunction, PlanMetrics, SupportsVeColBatch, VeFunction}
import com.nec.util.CallContext
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

import scala.language.dynamics

final case class VeOneStageEvaluationPlan(
  outputExpressions: Seq[NamedExpression],
  veFunction: VeFunction,
  child: SparkPlan
) extends SparkPlan
  with UnaryExecNode
  with LazyLogging
  with SupportsVeColBatch
  with PlanMetrics
  with PlanCallsVeFunction {

  require(outputExpressions.nonEmpty, "Expected OutputExpressions to be non-empty")

  override lazy val metrics = invocationMetrics(PLAN) ++ invocationMetrics(VE) ++ invocationMetrics(BATCH) ++ batchMetrics(INPUT) ++ batchMetrics(OUTPUT)

  override def output: Seq[Attribute] = outputExpressions.map(_.toAttribute)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def executeVeColumnar(): RDD[VeColBatch] = {
    initializeMetrics()

    child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
      .mapPartitions { veColBatches =>
        withVeLibrary { libRef =>
          logger.info(s"Will map batches with function ${veFunction}")

          incrementInvocations(PLAN)
          veColBatches.map { inputBatch =>
            collectBatchMetrics(INPUT, inputBatch)
            collectBatchMetrics(OUTPUT, withInvocationMetrics(BATCH){
              try {
                logger.debug(s"Mapping batch ${inputBatch}")
                val cols = withInvocationMetrics(VE){
                  vectorEngine.execute(
                    libRef,
                    veFunction.functionName,
                    inputBatch.columns.toList,
                    veFunction.namedResults
                  )
                }

                logger.debug(s"Completed mapping ${inputBatch}, got ${cols}")

                val outBatch = VeColBatch(cols)
                if (inputBatch.numRows < outBatch.numRows)
                  logger.error(s"Input rows = ${inputBatch.numRows}, output = ${outBatch}")
                outBatch
              } finally child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup(inputBatch)
            })
          }
        }
      }
  }

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(veFunction = f(veFunction))
}
