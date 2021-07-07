package com.nec.arrow.functions

import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.Float8VectorWrapper
import org.apache.arrow.vector.Float8Vector

object Sort {
  def runOn(nativeInterface: ArrowNativeInterfaceNumeric)(
    firstColumnVector: Float8Vector,
    outputVector: Float8Vector
  ): Unit = {

    outputVector.setValueCount(firstColumnVector.getValueCount())

    nativeInterface.callFunction(
      name = "sort_doubles",
      inputArguments = List(Some(Float8VectorWrapper(firstColumnVector)), None),
      outputArguments = List(None, Some(Float8VectorWrapper(outputVector)))
    )
  }

  def sortJVM(inputVector: Float8Vector): List[Double] =
    (0 until inputVector.getValueCount).map { idx =>
      inputVector.get(idx)
    }.toList.sorted
}
