package com.nec.arrow.functions

import com.nec.arrow.ArrowNativeInterfaceNumeric
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector

object Add {

  val PairwiseSumCode: String = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("add.c"))
    try source.mkString
    finally source.close()
  }

  def runOn(
    nativeInterface: ArrowNativeInterfaceNumeric
  )(firstColumnVector: Float8Vector, secondColumnvector: Float8Vector): Seq[Double] = {

    require(
      firstColumnVector.getValueCount == secondColumnvector.getValueCount,
      "Plugin only allows pairwise sum of columns that have the same size."
    )

    val ra = new RootAllocator()
    val outputVector = new Float8Vector("count", ra)
    outputVector.allocateNew(firstColumnVector.getValueCount)
    outputVector.setValueCount(firstColumnVector.getValueCount)

    nativeInterface.callFunction(
      name = "add",
      inputArguments = List(Some(firstColumnVector), Some(secondColumnvector), None),
      outputArguments = List(None, None, Some(outputVector))
    )

    (0 until outputVector.getValueCount).map { idx =>
      outputVector.getValueAsDouble(idx)
    }
  }

  def addJVM(firstColumnVector: Float8Vector, secondColumnVector: Float8Vector): Seq[Double] = {
    (0 until firstColumnVector.getValueCount).map { idx =>
      firstColumnVector.get(idx) + secondColumnVector.get(idx)
    }
  }
}
