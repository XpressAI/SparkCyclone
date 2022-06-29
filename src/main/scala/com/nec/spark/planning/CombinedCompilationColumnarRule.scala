package com.nec.spark.planning

import com.nec.native.{CompiledCodeInfo, NativeFunction}
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

  private[planning] def compileRawCodes(uncompiled: Seq[VeFunction]): Option[Path] = {
    val chunks = uncompiled.collect { case VeFunction(c: RawCode, name, _) => (name, c) }
      .toSet[(String, VeFunctionStatus.RawCode)].toSeq
      .sortBy(_._1)

    // Skip building an .SO if there are no uncompiled raw code chunks
    if (chunks.nonEmpty) {
      logger.info(s"There are ${chunks.size} uncompiled VeFunctions that are raw code chunks: ${chunks.map(_._1).mkString("[ ", ", ", " ]")}")

      // Combine the code chunks
      val combined = CodeStructure.combine(chunks.toSeq.map { chunk =>
        CodeStructure.from(CodeLines.parse(chunk._2.code))
      })

      // Build the code and return the .SO
      Some(SparkCycloneDriverPlugin.currentCompiler.build(combined.cCode))

    } else {
      Option.empty[Path]
    }
  }

  private[planning] def compileSourceCodes(uncompiled: Seq[VeFunction]): Map[Int, CompiledCodeInfo] = {
    val functions = uncompiled.collect { case VeFunction(c: SourceCode, _, _) => c.function }
      .toSet[NativeFunction].toSeq
      .sortBy(_.identifier)

    // Skip building / lookup if there are no uncompiled NativeFunctions
    if (functions.nonEmpty) {
      logger.info(s"There are ${functions.size} uncompiled VeFunctions that are NativeFunctions: ${functions.map(_.identifier).mkString("[ ", ", ", " ]")}")

      // Build the code and return the .SO
      SparkCycloneDriverPlugin.currentCompiler.build(functions.toSeq)

    } else {
      Map.empty[Int, CompiledCodeInfo]
    }
  }

  private[planning] def transformRawCodePlans(pathO: Option[Path]): PartialFunction[SparkPlan, SparkPlan] = { planT =>
    (planT, pathO) match {
      case (UncompiledPlan(plan), Some(path)) =>
        plan.updateVeFunction {
          case f @ VeFunction(_: RawCode, _, _) =>
            f.copy(status = Compiled(DistributedLibLocation(path.toString)))

          case other =>
            other
        }
    }
  }

  private[planning] def transformSourceCodePlans(cache: Map[Int, CompiledCodeInfo]): PartialFunction[SparkPlan, SparkPlan] = {
    case UncompiledPlan(plan) =>
      plan.updateVeFunction {
        case vefunc @ VeFunction(c: SourceCode, _, _) =>
          // Look up the cache by the native function's hashId
          cache.get(c.function.hashId) match {
            case Some(info) =>
              /*
                If the entry exists, replace the VeFunction status AND the name,
                since the already-compiled function with the same hashId was
                defined with another name
              */

              if (vefunc.name != info.name) {
                logger.debug(s"Mapping VEFunction name '${vefunc.name}' -> '${info.name}")
              }

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
        val soPathO = compileRawCodes(uncompiled)

        // Compile all VeFunctionStatus.SourceCode
        val funcCache = compileSourceCodes(uncompiled)

        logger.info(s"Finished compiling all VeFunctions; transforming existing plans...")

        // Apply transformations to the SparkPlan tree by replacing all uncompiled VeFunctions with comopiled variants
        plan
          .transformUp(transformRawCodePlans(soPathO))
          .transformUp(transformSourceCodePlans(funcCache))
      }

      logger.info(s"Plan compilation + transformation took ${duration.toDouble / 1e9}s")
      outplan

    } else {
      plan
    }
  }
}
