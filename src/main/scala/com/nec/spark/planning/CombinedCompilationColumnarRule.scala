package com.nec.spark.planning

import com.nec.native.CompiledCodeInfo
import com.nec.spark.SparkCycloneDriverPlugin
import com.nec.spark.agile.core.CodeLines
import com.nec.spark.agile.CodeStructure
import com.nec.spark.planning.LibLocation.DistributedLibLocation
import com.nec.spark.planning.PlanCallsVeFunction.UncompiledPlan
import com.nec.spark.planning.VeFunctionStatus._
import java.nio.file.Path
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

object CombinedCompilationColumnarRule extends ColumnarRule with LazyLogging {
  private[planning] def measureTime[T](thunk: => T): (T, Long) = {
    val start = System.nanoTime
    val result = thunk
    (result, System.nanoTime - start)
  }

  private[planning] def compileRawCodes(uncompiled: Seq[VeFunction]): Path = {
    val codes = uncompiled.collect { case VeFunction(c: RawCode, _, _) => c }.toSet
    logger.info(s"There are ${codes.size} uncompiled raw code chunks")

    // Combine the code chunks
    val combined = CodeStructure.combine(codes.toSeq.map { code =>
      CodeStructure.from(CodeLines.parse(code.code))
    })

    // Build the code and return the .SO
    SparkCycloneDriverPlugin.currentCompiler.build(combined.cCode)
  }

  private[planning] def compileSourceCodes(uncompiled: Seq[VeFunction]): Map[Int, CompiledCodeInfo] = {
    val functions = uncompiled.collect { case VeFunction(c: SourceCode, _, _) => c.function }.toSet
    logger.info(s"There are ${functions.size} uncompiled native functions")

    // Build the code and return the .SO
    SparkCycloneDriverPlugin.currentCompiler.build(functions.toSeq)
  }

  private[planning] def transformRawCodePlans(path: Path): PartialFunction[SparkPlan, SparkPlan] = {
    case UncompiledPlan(plan) =>
      plan.updateVeFunction {
        case f @ VeFunction(_: RawCode, _, _) =>
          f.copy(status = Compiled(DistributedLibLocation(path.toString)))

        case other =>
          other
      }
  }

  private[planning] def transformSourceCodePlans(cache: Map[Int, CompiledCodeInfo]): PartialFunction[SparkPlan, SparkPlan] = {
    case UncompiledPlan(plan) =>
      plan.updateVeFunction {
        case vefunc @ VeFunction(c: SourceCode, _, _) =>
          // Look up the cache by the native function's hashId
          cache.get(c.function.hashId) match {
            case Some(info) =>
              // If entry exists, replace the VeFunction status AND the name
              vefunc.copy(
                status = Compiled(DistributedLibLocation(info.path.toString)),
                name = info.name
              )

            case None =>
              vefunc
          }

        case other =>
          other
      }
  }

  override def preColumnarTransitions: Rule[SparkPlan] = { plan =>
    val uncompiled = plan.collect { case UncompiledPlan(plan) => plan.veFunction }

    if (uncompiled.nonEmpty) {
      val (outplan, duration) = measureTime {
        logger.info(s"Found ${uncompiled.length} uncompiled plans; proceeding to compile VE functions embedded in them")

        // Compile all VeFunctionStatus.RawCodes
        val soPath = compileRawCodes(uncompiled)

        // Compile all VeFunctionStatus.SourceCode
        val funcCache = compileSourceCodes(uncompiled)

        logger.info(s"Finished compiling all code into .SO files; transforming existing plans...")

        // Apply transformations to the SparkPlan tree by replacing all uncompiled VeFunction with comopiled variants
        plan
          .transformUp(transformRawCodePlans(soPath))
          .transformUp(transformSourceCodePlans(funcCache))
      }

      logger.info(s"Plan compilation + transformation took ${duration.toDouble / 1e9}s")
      outplan

    } else {
      plan
    }
  }
}
