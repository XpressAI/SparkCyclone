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

import com.nec.spark.SparkCycloneExecutorPlugin.{ImplicitMetrics, source, veProcess}
import com.nec.spark.planning.{PlanCallsVeFunction, PlanMetrics, SupportsVeColBatch, VeFunction}
import com.nec.ve.VeColBatch
import com.nec.ve.VeProcess.OriginalCallingContext
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

import scala.concurrent.duration.NANOSECONDS
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

  override lazy val metrics = invocationMetrics(PLAN) ++ invocationMetrics(VE) ++ invocationMetrics(BATCH) ++ batchMetrics(INPUT) ++ batchMetrics(OUTPUT)  ++ partitionMetrics(PLAN)

  override def output: Seq[Attribute] = outputExpressions.map(_.toAttribute)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def executeVeColumnar(): RDD[VeColBatch] = {
    val res =   child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
    res.mapPartitionsWithIndex { (index,veColBatches) =>
      collectPartitionMetrics(s"${index}${PLAN}",res.getNumPartitions)
      collectPartitionBatchSize(index,veColBatches.size)
      withVeLibrary { libRef =>
        logger.info(s"Will map batches with function ${veFunction}")
        import OriginalCallingContext.Automatic._

        incrementInvocations(PLAN)
        veColBatches.map { inputBatch =>
          collectBatchMetrics(INPUT, inputBatch)
          collectBatchMetrics(OUTPUT, withInvocationMetrics(BATCH){
            try {
              logger.debug(s"Mapping batch ${inputBatch}")
              val cols = withInvocationMetrics(VE){
                ImplicitMetrics.processMetrics.measureRunningTime(
                  veProcess.execute(
                    libraryReference = libRef,
                    functionName = veFunction.functionName,
                    cols = inputBatch.cols,
                    results = veFunction.namedResults
                  )
                )(
                  ImplicitMetrics.processMetrics
                    .registerFunctionCallTime(_, veFunction.functionName)
                )
              }

              logger.debug(s"Completed mapping ${inputBatch}, got ${cols}")

              val outBatch = VeColBatch.fromList(cols)
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
