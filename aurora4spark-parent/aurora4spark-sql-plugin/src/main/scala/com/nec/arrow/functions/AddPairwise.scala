package com.nec.arrow.functions

import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.Float8VectorWrapper
import org.apache.arrow.vector.Float8Vector

object AddPairwise {

  val PairwiseSumCode: String = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("add.c"))
    try source.mkString
    finally source.close()
  }

  def runOn(nativeInterface: ArrowNativeInterfaceNumeric)(
    firstColumnVector: Float8Vector,
    secondColumnvector: Float8Vector,
    outputVector: Float8Vector
  ): Unit = {

    require(
      firstColumnVector.getValueCount == secondColumnvector.getValueCount,
      "Plugin only allows pairwise sum of columns that have the same size."
    )

    // todo get this to be set by the C code
    outputVector.setValueCount(firstColumnVector.getValueCount())

    nativeInterface.callFunction(
      name = "add",
      inputArguments = List(
        Some(Float8VectorWrapper(firstColumnVector)),
        Some(Float8VectorWrapper(secondColumnvector)),
        None),
      outputArguments = List(None, None, Some(outputVector))
    )
  }

  def addJVM(firstColumnVector: Float8Vector, secondColumnVector: Float8Vector): List[Double] =
    (0 until firstColumnVector.getValueCount).map { idx =>
      firstColumnVector.get(idx) + secondColumnVector.get(idx)
    }.toList
}
