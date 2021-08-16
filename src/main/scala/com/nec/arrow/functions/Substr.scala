package com.nec.arrow.functions
import com.nec.arrow.ArrowNativeInterfaceNumeric.StackInput
import org.apache.arrow.vector.VarCharVector
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.VarCharVectorWrapper
import com.nec.arrow.CArrowNativeInterfaceNumeric

object Substr {
  def runOn(
    numeric: CArrowNativeInterfaceNumeric
  )(input: VarCharVector, output: VarCharVector, beginIndex: Int, endIndex: Int): Unit = {
    numeric.callFunction(
      name = "ve_substr",
      stackInputs = List(None, None, Some(StackInput(beginIndex)), Some(StackInput(endIndex))),
      inputArguments = List(Some(VarCharVectorWrapper(input)), None, None, None),
      outputArguments = List(None, Some(VarCharVectorWrapper(output)), None, None)
    )

  }

  val SourceCode: String = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("substr.c"))
    try source.mkString
    finally source.close()
  }
}
