package com.nec.arrow.functions
import org.apache.arrow.vector.Float8Vector
import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.Float8VectorWrapper
import org.apache.arrow.memory.RootAllocator

object Sum {

  val SumSourceCode: String = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("sum.c"))
    try source.mkString
    finally source.close()
  }

  def runOn(
    nativeInterface: ArrowNativeInterfaceNumeric
  )(float8Vector: Float8Vector, columnsCount: Int): Seq[Double] = {
    val ra = new RootAllocator(Long.MaxValue)
    val outputVector = new Float8Vector("count", ra)
    outputVector.allocateNew(columnsCount)
    outputVector.setValueCount(columnsCount)
    nativeInterface.callFunction(
      name = "sum_vectors",
      inputArguments = List(Some(Float8VectorWrapper(float8Vector)), None),
      outputArguments = List(None, Some(Float8VectorWrapper(outputVector)), None)
    )

    (0 until outputVector.getValueCount).map { idx =>
      outputVector.get(idx)
    }
  }

  def sumJVM(float8Vector: Float8Vector, columnsCount: Int): Seq[Double] = {
    if (float8Vector.getValueCount < 1) Seq.fill(columnsCount)(0)
    else
      (0 until float8Vector.getValueCount)
        .view
        .map(idx => float8Vector.get(idx))
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
