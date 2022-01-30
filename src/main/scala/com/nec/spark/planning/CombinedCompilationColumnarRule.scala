package com.nec.spark.planning

import com.nec.spark.SparkCycloneDriverPlugin
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CodeStructure
import com.nec.spark.planning.PlanCallsVeFunction.UncompiledPlan
import com.nec.spark.planning.VeFunction.VeFunctionStatus
import com.nec.spark.planning.VeFunction.VeFunctionStatus.SourceCode
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

import java.nio.file.Path
import java.time.Instant

object CombinedCompilationColumnarRule extends ColumnarRule with LazyLogging {
  override def preColumnarTransitions: Rule[SparkPlan] = { plan =>
    val uncompiledOnes = plan.collect { case UncompiledPlan(plan) =>
      plan
    }
    if (uncompiledOnes.nonEmpty) {
      val preMatchStart = Instant.now()
      logger.info(s"Found an uncompiled plan - proceeding.")

      logger.debug(s"Found ${uncompiledOnes.length} plans uncompiled")
      val compilationStart = Instant.now()

      val uncompiledCodes = uncompiledOnes
        .map(_.sparkPlan.veFunction)
        .collect { case VeFunction(sc@SourceCode(_), _, _) =>
          sc
        }
        .toSet

      logger.info(s"Found ${uncompiledCodes.size} codes uncompiled")

      val combined = CodeStructure.combine(uncompiledCodes.toList.map { sourceCode =>
        CodeStructure.from(CodeLines.parse(sourceCode.sourceCode))
      })

      val compiledPath: Path = SparkCycloneDriverPlugin.currentCompiler.forCode(combined.cCode)

      logger.info(s"Compiled all the code")

      val result = plan.transformUp { case UncompiledPlan(plan) =>
        plan.sparkPlan.updateVeFunction {
          case f@VeFunction(source@SourceCode(_), _, _) =>
            f.copy(veFunctionStatus = VeFunctionStatus.Compiled(compiledPath.toString))
          case other => other
        }
      }

      val compilationEnd = Instant.now()
      val timeTaken = java.time.Duration.between(compilationStart, compilationEnd)
      val timeTakenMatch = java.time.Duration.between(preMatchStart, compilationEnd)
      logger.info(
        s"Took ${timeTaken} to transform functions to compiled status (total: ${timeTakenMatch})."
      )
      logger.info(s"Compilation time: ${timeTakenMatch}")
      result
    } else plan
  }
}
