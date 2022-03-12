package com.nec.spark.agile.core

import com.nec.spark.agile.CFunction2
import com.nec.spark.agile.CFunctionGeneration.CVector
import com.nec.spark.planning.VeFunction
import com.nec.spark.planning.VeFunction.VeFunctionStatus

trait FunctionTemplateTrait {
  def name: String

  def outputs: List[CVector]

  def toCFunction: CFunction2

  final def toVeFunction: VeFunction = {
    VeFunction(
      VeFunctionStatus.fromCodeLines(toCFunction.toCodeLinesWithHeaders),
      name,
      outputs
    )
  }
}
