// package com.nec.spark.planning

// import com.nec.spark.{SparkCycloneDriverPlugin, SparkCycloneExecutorPlugin}
// import com.nec.spark.planning.PlanCallsVeFunction.UncompiledPlan
// import com.nec.spark.planning.VeFunctionStatus.RawCode
// import com.typesafe.scalalogging.LazyLogging

// import org.apache.spark.sql.catalyst.rules.Rule
// import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}
// import java.nio.file.Path
// import java.time.Instant

// import com.nec.spark.planning.LibLocation.DistributedLibLocation

// object ParallelCompilationColumnarRule extends ColumnarRule with LazyLogging {
//   override def preColumnarTransitions: Rule[SparkPlan] = { plan =>
//     val uncompiledOnes = plan.collect { case UncompiledPlan(plan) =>
//       plan
//     }
//     if (uncompiledOnes.nonEmpty) {
//       val preMatchStart = Instant.now()
//       logger.info(s"Found an uncompiled plan - proceeding.")

//       logger.debug(s"Found ${uncompiledOnes.length} plans uncompiled")
//       val compilationStart = Instant.now()

//       val uncompiledCodes = uncompiledOnes
//         .map(_.sparkPlan.veFunction)
//         .collect { case VeFunction(sc @ RawCode(_), _, _) =>
//           sc
//         }
//         .toSet

//       logger.info(s"Found ${uncompiledCodes.size} codes uncompiled")

//       val compiled: Map[RawCode, Path] = uncompiledCodes.toList.par
//         .map { sourceCode =>
//           sourceCode -> SparkCycloneDriverPlugin.currentCompiler.build(sourceCode.sourceCode)
//         }
//         .toMap
//         .toList
//         .toMap
//       logger.info(s"Compiled ${compiled.size} codes")

//       val result = plan.transformUp { case UncompiledPlan(plan) =>
//         plan.sparkPlan.updateVeFunction {
//           case f @ VeFunction(source @ RawCode(_), _, _) if compiled.contains(source) =>
//             f.copy(status =
//               VeFunctionStatus.Compiled(DistributedLibLocation(compiled(source).toString))
//             )
//           case other => other
//         }
//       }

//       val compilationEnd = Instant.now()
//       val timeTaken = java.time.Duration.between(compilationStart, compilationEnd)
//       val timeTakenMatch = java.time.Duration.between(preMatchStart, compilationEnd)
//       logger.info(
//         s"Took ${timeTaken} to transform functions to compiled status (total: ${timeTakenMatch})."
//       )
//       logger.info(s"Compilation time: ${timeTakenMatch}")
//       result
//     } else plan
//   }
// }
