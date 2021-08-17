package com.nec.arrow.functions

import com.nec.arrow.ArrowNativeInterface
import com.nec.arrow.ArrowNativeInterface.NativeArgument
import org.apache.arrow.vector.Float8Vector

object Sort {
  def runOn(
    nativeInterface: ArrowNativeInterface
  )(firstColumnVector: Float8Vector, outputVector: Float8Vector): Unit = {

    outputVector.setValueCount(firstColumnVector.getValueCount())

    nativeInterface.callFunctionWrapped(
      "sort_doubles",
      List(NativeArgument.input(firstColumnVector), NativeArgument.output(outputVector))
    )
  }

  def sortJVM(inputVector: Float8Vector): List[Double] =
    (0 until inputVector.getValueCount)
      .map { idx =>
        inputVector.get(idx)
      }
      .toList
      .sorted
}
