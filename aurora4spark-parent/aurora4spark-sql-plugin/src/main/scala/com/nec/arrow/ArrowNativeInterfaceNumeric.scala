package com.nec.arrow

import org.apache.arrow.vector._
import com.nec.arrow.ArrowNativeInterfaceNumeric._
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper._

trait ArrowNativeInterfaceNumeric extends Serializable {
  final def callFunction(
    name: String,
    inputArguments: List[Option[SupportedVectorWrapper]],
    outputArguments: List[Option[Float8Vector]]
  ): Unit = {
    try {
      callFunctionGen(
        name = name,
        inputArguments = inputArguments,
        outputArguments = outputArguments.map(_.map(f8v => Float8VectorWrapper(f8v)))
      )
    } catch {
      case e: Throwable =>
        throw new RuntimeException(s"Failed: inputs = ${inputArguments}; ${e}", e)
    }
  }

  def callFunctionGen(
    name: String,
    inputArguments: List[Option[SupportedVectorWrapper]],
    outputArguments: List[Option[SupportedVectorWrapper]]
  )
}
object ArrowNativeInterfaceNumeric {
  sealed trait SupportedVectorWrapper {}
  object SupportedVectorWrapper {
    final case class Float8VectorWrapper(float8Vector: Float8Vector) extends SupportedVectorWrapper
    final case class IntVectorWrapper(intVector: IntVector) extends SupportedVectorWrapper

  }
}
