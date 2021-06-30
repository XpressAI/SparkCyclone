package com.nec.arrow.functions
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.Float8VectorWrapper
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.IntVectorWrapper
import com.nec.arrow.ArrowNativeInterfaceNumeric
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.Float8Vector

object Join {

  val JoinSourceCode: String = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/com/nec/arrow/functions/cpp/joiner.cc"))
    try source.mkString
    finally source.close()
  }

  def runOn(nativeInterface: ArrowNativeInterfaceNumeric)(
    leftValuesVector: Float8Vector,
    rightValuesVector: Float8Vector,
    leftKeyVector: IntVector,
    rightKeyVector: IntVector,
    outputVector: Float8Vector
  ): Unit = {

    nativeInterface.callFunction(
      name = "join_doubles",
      inputArguments = List(
        Some(Float8VectorWrapper(leftValuesVector)),
        Some(Float8VectorWrapper(rightValuesVector)),
        Some(IntVectorWrapper(leftKeyVector)),
        Some(IntVectorWrapper(rightKeyVector)),
        None
      ),
      outputArguments = List(None, None , None, None, Some(outputVector))
    )
  }

  def joinJVM(leftColumn: Float8Vector, rightColumn: Float8Vector,
              leftKey: IntVector, rightKey: IntVector): List[(Double, Double)] = {
    val leftColVals = (0 until leftColumn.getValueCount).map(idx => leftColumn.get(idx))
    val rightColVals = (0 until rightColumn.getValueCount).map(idx => rightColumn.get(idx))
    val leftKeyVals = (0 until leftKey.getValueCount).map(idx => leftKey.get(idx))
    val rightKeyVals = (0 until rightKey.getValueCount).map(idx => rightKey.get(idx))
    val leftMap = leftKeyVals.zip(leftColVals).toMap
    val rightMap = rightKeyVals.zip(rightColVals).toMap
    val joinedKeys = leftKeyVals.filter(key => rightMap.contains(key))
    joinedKeys.map(key => leftMap(key)).zip(
      joinedKeys.map(key => rightMap(key))
    ).toList
  }
}
