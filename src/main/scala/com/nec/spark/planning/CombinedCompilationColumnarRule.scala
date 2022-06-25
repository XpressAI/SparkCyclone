package com.nec.spark.planning

import com.nec.spark.SparkCycloneDriverPlugin
import com.nec.spark.agile.core.CodeLines
import com.nec.spark.agile.CodeStructure
import com.nec.spark.planning.LibLocation.DistributedLibLocation
import com.nec.spark.planning.PlanCallsVeFunction.UncompiledPlan
import com.nec.spark.planning.VeFunctionStatus._
import com.typesafe.scalalogging.LazyLogging
import java.nio.file.Path
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

object CombinedCompilationColumnarRule extends ColumnarRule with LazyLogging {
  private[planning] def measureTime[T](thunk: => T): (T, Long) = {
    val start = System.nanoTime
    val result = thunk
    (result, System.nanoTime - start)
  }

  private[planning] def compileRawCodes(uncompiled: Seq[VeFunction]): Path = {
    val codes = uncompiled.collect { case VeFunction(c: SourceCode, _, _) => c }.toSet
    logger.info(s"There are ${codes.size} uncompiled raw code chunks")

    // Combine the code chunks
    val combined = CodeStructure.combine(codes.toSeq.map { code =>
      CodeStructure.from(CodeLines.parse(code.code))
    })

    // Build the code and return the .SO
    SparkCycloneDriverPlugin.currentCompiler.build(combined.cCode)
  }

  private[planning] def compileNativeFunctions(uncompiled: Seq[VeFunction]): Map[Int, Path] = {
    val functions = uncompiled.collect { case VeFunction(n: NativeFunctions, _, _) => n.functions }.flatten.toSet
    logger.info(s"There are ${functions.size} uncompiled native functions")

    // Build the code and return the .SO
    SparkCycloneDriverPlugin.currentCompiler.build(functions.toSeq)
  }

  private[planning] def transformRawCodePlans(path: Path): PartialFunction[SparkPlan, SparkPlan] = {
    case UncompiledPlan(plan) =>
      plan.updateVeFunction {
        case f @ VeFunction(_: SourceCode, _, _) =>
          f.copy(status = Compiled(DistributedLibLocation(path.toString)))

        case other =>
          other
      }
  }

  // private[planning] def transformNativeFunctionPlans(cache: Map[Int, Path]): PartialFunction[SparkPlan, SparkPlan] = {
  //   case UncompiledPlan(plan) =>
  //     plan.updateVeFunction {
  //       case f @ VeFunction(n: NativeFunctions, _, _) =>
  //         f.copy(status = Compiled(DistributedLibLocation(path.toString)))

  //       case other =>
  //         other
  //     }
  // }

  override def preColumnarTransitions: Rule[SparkPlan] = { plan =>
    val uncompiled = plan.collect { case UncompiledPlan(plan) => plan.veFunction }

    if (uncompiled.nonEmpty) {
      val (outplan, duration) = measureTime {
        logger.info(s"Found ${uncompiled.length} uncompiled plans; proceeding to compile VE functions embedded in them")

        val soPath = compileRawCodes(uncompiled)
        logger.info(s"Finished compiling .SO files; transforming existing plans...")

        plan.transformUp(transformRawCodePlans(soPath))
      }

      logger.info(s"Plan compilation + transformation took ${duration.toDouble / 1e9}s")
      outplan

    } else {
      plan
    }
  }
}
