package com.nec.arrow.functions

import com.nec.arrow.ArrowNativeInterface
import com.nec.arrow.ArrowNativeInterface.NativeArgument
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.VarCharVectorInputWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorOutputNativeArgument.OutputVectorWrapper.VarCharVectorOutputWrapper
import com.nec.arrow.ArrowNativeInterface.ScalarInput
import org.apache.arrow.vector.VarCharVector

object Substr {
  def runOn(
    numeric: ArrowNativeInterface
  )(input: VarCharVector, output: VarCharVector, beginIndex: Int, endIndex: Int): Unit = {
    numeric.callFunctionWrapped(
      name = "ve_substr",
      arguments = List(
        NativeArgument.VectorInputNativeArgument(VarCharVectorInputWrapper(input)),
        NativeArgument.VectorOutputNativeArgument(VarCharVectorOutputWrapper(output)),
        NativeArgument.ScalarInputNativeArgument(ScalarInput(beginIndex)),
        NativeArgument.ScalarInputNativeArgument(ScalarInput(endIndex))
      )
    )
  }

  val SourceCode: String = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("substr.cpp"))
    try source.mkString
    finally source.close()
  }
}
