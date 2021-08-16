package com.nec.arrow.functions
import org.apache.arrow.vector.VarCharVector
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.VarCharVectorWrapper
import com.nec.arrow.CArrowNativeInterfaceNumeric

object Substr {
  def runOn(
    numeric: CArrowNativeInterfaceNumeric
  )(input: VarCharVector, output: VarCharVector): Unit = {
    numeric.callFunction(
      name = "ve_substr",
      inputArguments = List(Some(VarCharVectorWrapper(input)), None),
      outputArguments = List(None, Some(VarCharVectorWrapper(output)))
    )

  }

  val SourceCode: String = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("substr.c"))
    try source.mkString
    finally source.close()
  }
}
