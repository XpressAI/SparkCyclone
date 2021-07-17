package com.nec.arrow

import org.apache.arrow.vector._
import com.nec.arrow.ArrowNativeInterfaceNumeric._
import com.typesafe.scalalogging.LazyLogging

import java.nio.ByteBuffer

trait ArrowNativeInterfaceNumeric extends Serializable with LazyLogging {
  final def callFunction(
    name: String,
    inputArguments: List[Option[SupportedVectorWrapper]],
    outputArguments: List[Option[SupportedVectorWrapper]]
  ): Unit = {
    try {
      val startTime = System.currentTimeMillis()
      logger.debug(s"Calling '${name}''")
      logger.whenTraceEnabled {
        logger.trace(s"Input is: ${inputArguments}")
      }
      val result = callFunctionGen(
        name = name,
        inputArguments = inputArguments,
        outputArguments = outputArguments
      )
      val endTime = System.currentTimeMillis()
      logger.whenTraceEnabled {
        logger.trace(s"Output is: ${outputArguments}")
      }
      logger.debug(s"Took ${endTime - startTime}ms to execute '$name'.")
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
    final case class VarCharVectorWrapper(varCharVector: VarCharVector)
      extends SupportedVectorWrapper
    final case class ByteBufferWrapper(byteBuffer: ByteBuffer, size: Int)
      extends SupportedVectorWrapper
    final case class Float8VectorWrapper(float8Vector: Float8Vector) extends SupportedVectorWrapper
    final case class IntVectorWrapper(intVector: IntVector) extends SupportedVectorWrapper
    final case class BigIntVectorWrapper(bigIntVector: BigIntVector) extends SupportedVectorWrapper
  }

  final case class DeferredArrowInterfaceNumeric(subInterface: () => ArrowNativeInterfaceNumeric)
    extends ArrowNativeInterfaceNumeric {
    override def callFunctionGen(
      name: String,
      inputArguments: List[Option[SupportedVectorWrapper]],
      outputArguments: List[Option[SupportedVectorWrapper]]
    ): Unit = subInterface().callFunction(name, inputArguments, outputArguments)
  }
}
