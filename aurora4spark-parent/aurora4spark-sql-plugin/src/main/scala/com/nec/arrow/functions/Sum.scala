package com.nec.arrow.functions
import org.apache.arrow.vector.Float8Vector
import com.nec.arrow.ArrowNativeInterfaceNumeric
import org.apache.arrow.memory.RootAllocator

object Sum {

  val SumSourceCode: String = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("sum.c"))
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
      name = "sum_vectors",
      inputArguments = List(Some(varCharVector), None),
      outputArguments = List(None, Some(outputVector), None)
    )

    (0 until outputVector.getValueCount).map { idx =>
      outputVector.getValueAsDouble(idx)
    }
  }

  def sumJVM(float8Vector: Float8Vector, columnsCount: Int): Seq[Double] = {
    (0 until float8Vector.getValueCount)
      .map(idx => float8Vector.getValueAsDouble(idx))
      .zipWithIndex
      .groupBy { case (elem, idx) =>
        idx % columnsCount
      }
      .map { case (idx, seq) =>
        seq.map(_._1).sum
      }
      .toSeq
  }

}
