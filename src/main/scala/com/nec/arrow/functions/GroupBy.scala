package com.nec.arrow.functions

import com.nec.arrow.ArrowNativeInterface
import com.nec.arrow.ArrowNativeInterface.NativeArgument
import org.apache.arrow.vector.Float8Vector

object GroupBy {

  val GroupBySourceCode: String = {
    val source = scala.io.Source.fromInputStream(
      getClass.getResourceAsStream("/com/nec/arrow/functions/cpp/grouper.cc")
    )
    try source.mkString
    finally source.close()
  }

  def runOn(nativeInterface: ArrowNativeInterface)(
    groupingVector: Float8Vector,
    valuesVector: Float8Vector,
    outputGroupsVector: Float8Vector,
    outputGroupsCountVector: Float8Vector,
    outputValuesVector: Float8Vector
  ): Unit = {

    nativeInterface.callFunctionWrapped(
      "group_by",
      List(
        NativeArgument.input(groupingVector),
        NativeArgument.input(valuesVector),
        NativeArgument.output(outputValuesVector),
        NativeArgument.output(outputGroupsVector),
        NativeArgument.output(outputGroupsCountVector)
      )
    )
  }

  def groupJVM(
    groupingVector: Float8Vector,
    valuesVector: Float8Vector
  ): Map[Double, Seq[Double]] = {
    val groupingVals = (0 until groupingVector.getValueCount).map(idx => groupingVector.get(idx))
    val valuesVals = (0 until valuesVector.getValueCount).map(idx => valuesVector.get(idx))

    groupingVals
      .zip(valuesVals)
      .groupBy(_._1)
      .map(kv => (kv._1, kv._2.map(_._2)))

  }
}
