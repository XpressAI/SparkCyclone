package com.nec.arrow.functions

import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.ArrowNativeInterfaceNumeric.ScalarInput
import org.apache.arrow.vector.VarCharVector
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.VarCharVectorWrapper

object Substr {
  def runOn(
    numeric: ArrowNativeInterfaceNumeric
  )(input: VarCharVector, output: VarCharVector, beginIndex: Int, endIndex: Int): Unit = {
    numeric.callFunction(
      name = "ve_substr",
      scalarInputs = List(None, None, Some(ScalarInput(beginIndex)), Some(ScalarInput(endIndex))),
      inputArguments = List(Some(VarCharVectorWrapper(input)), None, None, None),
      outputArguments = List(None, Some(VarCharVectorWrapper(output)), None, None)
    )

  }

  val SourceCode: String = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("substr.cpp"))
    try source.mkString
    finally source.close()
  }
}
