package com.nec

import scala.language.higherKinds

import com.nec.native.{ArrowNativeInterface, ArrowNativeInterfaceNumeric}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{Float8Vector, IntVector, VarCharVector}

object Sum {

  val SumSourceCode: String = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/sum.c"))
    try source.mkString
    finally source.close()
  }

  def runOn(
    nativeInterface: ArrowNativeInterfaceNumeric
  )(varCharVector: Float8Vector, columnsCount: Int): Seq[Double] = {
    val ra = new RootAllocator()
    val outputVector = new Float8Vector("count", ra)
    outputVector.allocateNew(columnsCount)
    outputVector.setValueCount(columnsCount)
    nativeInterface.callFunction(
      name = "sum",
      inputArguments = List(Some(varCharVector), None),
      outputArguments = List(None, Some(outputVector), None)
    )

    (0 until outputVector.getValueCount).map { idx =>
      outputVector.getValueAsDouble(idx)
    }
  }

  def sumJVM(float8Vector: Float8Vector, columnsCount: Int): Seq[Double] = {
    val groupSize = float8Vector.getValueCount/columnsCount;
    (0 until columnsCount).map{ colIndex =>
      (0 until groupSize).map { elemIndex =>
        float8Vector.getValueAsDouble(colIndex  + elemIndex * groupSize)

      }
    }.map(_.sum)
  }

}
