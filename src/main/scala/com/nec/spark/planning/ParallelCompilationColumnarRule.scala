package com.nec.spark.planning

import com.nec.spark.{SparkCycloneDriverPlugin, SparkCycloneExecutorPlugin}
import com.nec.spark.planning.PlanCallsVeFunction.UncompiledPlan
import com.nec.spark.planning.VeFunction.VeFunctionStatus
import com.nec.spark.planning.VeFunction.VeFunctionStatus.SourceCode
import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}
import java.nio.file.Path
import java.time.Instant

import com.nec.spark.planning.LibLocation.DistributedLibLocation

object ParallelCompilationColumnarRule extends ColumnarRule with LazyLogging {
  override def preColumnarTransitions: Rule[SparkPlan] = { plan =>
    val uncompiledOnes = plan.collect { case UncompiledPlan(plan) =>
      plan
    }
    if (uncompiledOnes.nonEmpty) {
      val preMatchStart = Instant.now()
      logger.info("Found an uncompiled plan - proceeding.")

      logger.debug("Found {} plans uncompiled", uncompiledOnes.length)
      val compilationStart = Instant.now()

      val uncompiledCodes = uncompiledOnes
        .map(_.sparkPlan.veFunction)
        .collect { case VeFunction(sc @ SourceCode(_), _, _) =>
          sc
        }
        .toSet

      logger.info("Found {} codes uncompiled", uncompiledCodes.size)

      val compiled: Map[SourceCode, Path] = uncompiledCodes.toList.par
        .map { sourceCode =>
          sourceCode -> SparkCycloneDriverPlugin.currentCompiler.forCode(sourceCode.sourceCode)
        }
        .toMap
        .toList
        .toMap
      logger.info("Compiled {} codes", compiled.size)

      val result = plan.transformUp { case UncompiledPlan(plan) =>
        plan.sparkPlan.updateVeFunction {
          case f @ VeFunction(source @ SourceCode(_), _, _) if compiled.contains(source) =>
            f.copy(veFunctionStatus =
              VeFunctionStatus.Compiled(DistributedLibLocation(compiled(source).toString))
            )
          case other => other
        }
      }

      val compilationEnd = Instant.now()
      val timeTaken = java.time.Duration.between(compilationStart, compilationEnd)
      val timeTakenMatch = java.time.Duration.between(preMatchStart, compilationEnd)
      logger.info(
        "Took {} to transform functions to compiled status (total: {}).",
        timeTaken, timeTakenMatch
      )
      logger.info("Compilation time: {}", timeTakenMatch)
      result
    } else plan
  }
}
