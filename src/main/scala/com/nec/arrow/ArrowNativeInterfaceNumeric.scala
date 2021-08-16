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
  ): Unit = callFunction(
    name = name,
    stackInputs = List.fill(Math.max(inputArguments.size, outputArguments.size))(None),
    inputArguments = inputArguments,
    outputArguments = outputArguments
  )

  final def callFunction(
    name: String,
    stackInputs: List[Option[SupportedStackInput]],
    inputArguments: List[Option[SupportedVectorWrapper]],
    outputArguments: List[Option[SupportedVectorWrapper]]
  ): Unit = {
    try {
      val startTime = System.currentTimeMillis()
      logger.debug(s"Calling '${name}''")
      logger.whenTraceEnabled {
        logger.trace(s"Input is: ${inputArguments}")
      }
      callFunctionGen(
        name = name,
        stackInputs = stackInputs,
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
    stackInputs: List[Option[SupportedStackInput]],
    inputArguments: List[Option[SupportedVectorWrapper]],
    outputArguments: List[Option[SupportedVectorWrapper]]
  ): Unit

}
object ArrowNativeInterfaceNumeric {
  object StackInput {
    def apply(int: Int): SupportedStackInput = SupportedStackInput.ForInt(int)
  }
  sealed trait SupportedStackInput {}
  object SupportedStackInput {
    case class ForInt(value: Int) extends SupportedStackInput
  }
  sealed trait SupportedVectorWrapper {}
  object SupportedVectorWrapper {
    def wrapVector(valueVector: ValueVector): SupportedVectorWrapper = {
      valueVector match {
        case intVector: IntVector         => IntVectorWrapper(intVector)
        case float8Vecot: Float8Vector    => Float8VectorWrapper(float8Vecot)
        case varcharVector: VarCharVector => VarCharVectorWrapper(varcharVector)
        case bigintVector: BigIntVector   => BigIntVectorWrapper(bigintVector)
      }
    }
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
      stackInputs: List[Option[SupportedStackInput]],
      inputArguments: List[Option[SupportedVectorWrapper]],
      outputArguments: List[Option[SupportedVectorWrapper]]
    ): Unit = subInterface().callFunction(name, stackInputs, inputArguments, outputArguments)
  }
}
