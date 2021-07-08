package com.nec.arrow

import org.apache.arrow.vector._
import com.nec.arrow.ArrowNativeInterfaceNumeric._
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper._

import java.nio.ByteBuffer

trait ArrowNativeInterfaceNumeric extends Serializable {
  final def callFunction(
    name: String,
    inputArguments: List[Option[SupportedVectorWrapper]],
    outputArguments: List[Option[SupportedVectorWrapper]]
  ): Unit = {
    try {
      callFunctionGen(
        name = name,
        inputArguments = inputArguments,
        outputArguments = outputArguments
      )
    } catch {
      case e: Throwable =>
        throw new RuntimeException(
          s"Failed to execute ${name}: inputs = ${inputArguments}; ${e}",
          e
        )
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
    final case class StringWrapper(string: String) extends SupportedVectorWrapper
    final case class ByteBufferWrapper(byteBuffer: ByteBuffer, size: Int) extends SupportedVectorWrapper
    final case class Float8VectorWrapper(float8Vector: Float8Vector) extends SupportedVectorWrapper
    final case class IntVectorWrapper(intVector: IntVector) extends SupportedVectorWrapper
  }
}
