package com.nec.spark.planning

import com.nec.spark.SparkCycloneDriverPlugin
import com.nec.spark.planning.OneStageEvaluationPlan.VeFunction
import com.nec.spark.planning.OneStageEvaluationPlan.VeFunction.VeFunctionStatus
import com.nec.spark.planning.OneStageEvaluationPlan.VeFunction.VeFunctionStatus.SourceCode
import com.nec.spark.planning.PlanCallsVeFunction.UncompiledPlan
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

import java.nio.file.Path

object ParallelCompilationColumnarRule extends ColumnarRule with LazyLogging {
  override def preColumnarTransitions: Rule[SparkPlan] = { plan =>
    val uncompiledOnes = plan.collect { case UncompiledPlan(plan) =>
      plan
    }

    logger.debug(s"Found ${uncompiledOnes.length} plans uncompiled")
    if (uncompiledOnes.nonEmpty) {

      val uncompiledCodes = uncompiledOnes
        .map(_.sparkPlan.veFunction)
        .collect { case VeFunction(sc @ SourceCode(_), _, _) =>
          sc
        }
        .toSet

      logger.info(s"Found ${uncompiledCodes.size} codes uncompiled")

      val compiled: Map[SourceCode, Path] = uncompiledCodes.toList.par
        .map { sourceCode =>
          sourceCode -> SparkCycloneDriverPlugin.currentCompiler.forCode(sourceCode.sourceCode)
        }
        .toMap
        .toList
        .toMap
      logger.info(s"Compiled ${compiled.size} codes")

      val result = plan.transformUp { case UncompiledPlan(plan) =>
        plan.sparkPlan.updateVeFunction {
          case f @ VeFunction(source @ SourceCode(_), _, _) if compiled.contains(source) =>
            f.copy(veFunctionStatus = VeFunctionStatus.Compiled(compiled(source).toString))
          case other => other
        }
      }
      logger.info(s"Transformed functions to compiled status")
      result
    } else plan
  }
}
