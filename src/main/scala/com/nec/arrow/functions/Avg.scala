package com.nec.arrow.functions

import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.Float8VectorWrapper
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.memory.RootAllocator

object Avg {

  val AvgSourceCode: String = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("avg.c"))
    try source.mkString
    finally source.close()
  }

  def runOn(
    nativeInterface: ArrowNativeInterfaceNumeric
  )(float8Vector: Float8Vector, columnsCount: Int): Seq[Double] = {
    val ra = new RootAllocator()
    val outputVector = new Float8Vector("count", ra)
    outputVector.allocateNew(columnsCount)
    outputVector.setValueCount(columnsCount)
    nativeInterface.callFunction(
      name = "vector_avg",
      inputArguments = List(Some(Float8VectorWrapper(float8Vector)), None),
      outputArguments = List(None, Some(Float8VectorWrapper(outputVector)), None)
    )

    (0 until outputVector.getValueCount).map { idx =>
      outputVector.getValueAsDouble(idx)
    }
  }

  def avgJVM(float8Vector: Float8Vector, columnsCount: Int): Seq[Double] = {
    (0 until float8Vector.getValueCount)
      .map(idx => float8Vector.getValueAsDouble(idx))
      .zipWithIndex
      .groupBy { case (elem, idx) =>
        idx % columnsCount
      }
      .map { case (idx, seq) =>
        (seq.map(_._1).sum) / seq.size
      }
      .toSeq
  }

}
