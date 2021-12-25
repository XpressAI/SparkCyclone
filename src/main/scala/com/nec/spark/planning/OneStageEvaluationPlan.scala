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
package com.nec.spark.planning

import com.nec.cmake.{ScalaTcpDebug, Spanner}
import com.nec.spark.SparkCycloneExecutorPlugin.{source, veProcess}
import com.nec.spark.agile.CFunctionGeneration.VeType
import com.nec.spark.planning.OneStageEvaluationPlan.VeFunction
import com.nec.spark.planning.OneStageEvaluationPlan.VeFunction.VeFunctionStatus
import com.nec.ve.VeColBatch
import com.nec.ve.VeKernelCompiler.VeCompilerConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkEnv
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

import scala.language.dynamics

object OneStageEvaluationPlan {

  object VeFunction {
    sealed trait VeFunctionStatus
    object VeFunctionStatus {
      final case class SourceCode(sourceCode: String) extends VeFunctionStatus {
        override def toString: String = super.toString.take(25)
      }
      final case class Compiled(libraryPath: String) extends VeFunctionStatus
    }
  }

  final case class VeFunction(
    veFunctionStatus: VeFunctionStatus,
    functionName: String,
    results: List[VeType]
  ) {
    def isCompiled: Boolean = veFunctionStatus match {
      case VeFunctionStatus.SourceCode(_) => false
      case VeFunctionStatus.Compiled(_)   => true
    }
    def libraryPath: String = veFunctionStatus match {
      case VeFunctionStatus.SourceCode(path) =>
        sys.error(s"Library was not compiled; expected to build from ${path
          .take(10)} (${path.hashCode})... Does your plan extend ${classOf[PlanCallsVeFunction]}?")
      case VeFunctionStatus.Compiled(libraryPath) => libraryPath
    }
  }
}

final case class OneStageEvaluationPlan(
  outputExpressions: Seq[NamedExpression],
  veFunction: VeFunction,
  child: SparkPlan
) extends SparkPlan
  with UnaryExecNode
  with LazyLogging
  with SupportsVeColBatch
  with PlanCallsVeFunction {

  require(outputExpressions.nonEmpty, "Expected OutputExpressions to be non-empty")

  override def output: Seq[Attribute] = outputExpressions.map(_.toAttribute)

  private val copiedRefs = outputExpressions.filter(_.isInstanceOf[AttributeReference])

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def executeVeColumnar(): RDD[VeColBatch] = {
    val debug = ScalaTcpDebug(VeCompilerConfig.fromSparkConf(sparkContext.getConf))
    val tracer = Tracer.Launched.fromSparkContext(sparkContext)
    child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
      .mapPartitions { veColBatches =>
        Spanner(debug, tracer.mappedSparkEnv(SparkEnv.get)).spanIterator("map col batches") {
          withVeLibrary { libRef =>
            logger.info(s"Will map batches with function ${veFunction}")
            veColBatches.map { veColBatch =>
              try {
                logger.debug(s"Mapping batch ${veColBatch}")
                val cols = veProcess.execute(
                  libraryReference = libRef,
                  functionName = veFunction.functionName,
                  cols = veColBatch.cols,
                  results = veFunction.results
                )
                logger.debug(s"Completed mapping ${veColBatch}, got ${cols}")

                val outBatch = VeColBatch.fromList(cols)
                if (veColBatch.numRows < outBatch.numRows)
                  logger.error(s"Input rows = ${veColBatch.numRows}, output = ${outBatch}")
                outBatch
              } finally child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup(veColBatch)
            }
          }
        }
      }
  }

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(veFunction = f(veFunction))
}
