package io.sparkcyclone.spark.plans

import io.sparkcyclone.spark.planning.VeFunction
import io.sparkcyclone.vectorengine.{LibraryReference, VeProcess}
import org.apache.spark.sql.execution.SparkPlan

object PlanCallsVeFunction {
  object UncompiledPlan {
    def unapply(plan: SparkPlan): Option[SparkPlan with PlanCallsVeFunction] = {
      PartialFunction.condOpt(plan) {
        case p: SparkPlan with PlanCallsVeFunction if !p.veFunction.isCompiled => p
      }
    }
  }
}

trait PlanCallsVeFunction {
  def veFunction: VeFunction

  def updateVeFunction(thunk: VeFunction => VeFunction): SparkPlan

  def withVeLibrary[T](thunk: LibraryReference => T)(implicit veProcess: VeProcess): T = {
    thunk(veProcess.load(veFunction.libraryPath))
  }
}
