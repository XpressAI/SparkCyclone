package com.nec.arrow

import org.apache.arrow.vector._
import ArrowNativeInterfaceNumeric._
import SupportedVectorWrapper._

trait ArrowNativeInterfaceNumeric extends Serializable {
  final def callFunction(
    name: String,
    inputArguments: List[Option[Float8Vector]],
    outputArguments: List[Option[Float8Vector]]
  ): Unit = {
    callFunctionGen(
      name = name, 
      inputArguments = inputArguments.map(_.map(f8v => Float8VectorWrapper(f8v))),
      outputArguments = outputArguments.map(_.map(f8v => Float8VectorWrapper(f8v)))
    )
  }

  def callFunctionGen(
    name: String,
    inputArguments: List[Option[SupportedVectorWrapper]],
    outputArguments: List[Option[SupportedVectorWrapper]]
  )
}
object ArrowNativeInterfaceNumeric {
  sealed trait SupportedVectorWrapper {
  }
  object SupportedVectorWrapper {
    final case class Float8VectorWrapper(float8Vector: Float8Vector) extends SupportedVectorWrapper
    final case class IntVectorWrapper(intVector: IntVector) extends SupportedVectorWrapper

  }
}
