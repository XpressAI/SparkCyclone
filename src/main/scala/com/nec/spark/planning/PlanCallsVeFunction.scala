package com.nec.spark.planning

import com.nec.spark.planning.OneStageEvaluationPlan.VeFunction
import com.nec.ve.VeProcess
import com.nec.ve.VeProcess.LibraryReference
import org.apache.spark.sql.execution.SparkPlan

import java.nio.file.Paths

object PlanCallsVeFunction {
  final case class UncompiledPlan(sparkPlan: SparkPlan with PlanCallsVeFunction)
  object UncompiledPlan {
    def unapply(sparkPlan: SparkPlan): Option[UncompiledPlan] = PartialFunction.condOpt(sparkPlan) {
      case p: SparkPlan with PlanCallsVeFunction if !p.veFunction.isCompiled =>
        UncompiledPlan(p)
    }
  }
}
trait PlanCallsVeFunction {
  def veFunction: VeFunction
  def updateVeFunction(f: VeFunction => VeFunction): SparkPlan

  def withVeLibrary[T](f: LibraryReference => T)(implicit veProcess: VeProcess): T =
    f(veProcess.loadLibrary(Paths.get(veFunction.libraryPath)))

}
