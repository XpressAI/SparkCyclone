package com.nec.arrow

import com.nec.arrow.ArrowNativeInterface.NativeArgument.ScalarInputNativeArgument
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.StringInputWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorOutputNativeArgument
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorOutputNativeArgument.OutputVectorWrapper
import org.apache.arrow.vector._
import com.nec.arrow.ArrowNativeInterface._
import com.typesafe.scalalogging.LazyLogging

import java.nio.ByteBuffer
import scala.language.implicitConversions

trait ArrowNativeInterface extends Serializable with LazyLogging {

  def callFunctionWrapped(name: String, arguments: List[NativeArgument]): Unit

}

object ArrowNativeInterface {

  final case class LoggingArrowNativeInterface(underlying: ArrowNativeInterface)
    extends ArrowNativeInterface
    with LazyLogging {
    override def callFunctionWrapped(name: String, arguments: List[NativeArgument]): Unit = {
      try {
        val startTime = System.currentTimeMillis()
        logger.debug(s"Calling '$name''")
        logger.whenTraceEnabled {
          logger.trace(s"Arguments are: ${arguments}")
        }
        underlying.callFunctionWrapped(name = name, arguments = arguments)
        val endTime = System.currentTimeMillis()
        logger.whenTraceEnabled {
          logger.trace(s"Resulting arguments are: ${arguments}")
        }
        logger.debug(s"Took ${endTime - startTime}ms to execute '$name'.")
      } catch {
        case e: Throwable =>
          throw new RuntimeException(s"Failed to execute ${name}: arguments = $arguments; $e", e)
      }
    }
  }

  implicit class RichArrowInterfaceNumeric(arrowNativeInterfaceNumeric: ArrowNativeInterface) {
    def callFunctionGen(
      name: String,
      scalarInputs: List[Option[ScalarInput]],
      inputArguments: List[Option[InputVectorWrapper]],
      outputArguments: List[Option[OutputVectorWrapper]]
    ): Unit = {
      arrowNativeInterfaceNumeric.callFunctionWrapped(
        name,
        arguments = scalarInputs
          .map(_.map(ScalarInputNativeArgument(_)))
          .zip(inputArguments.map(_.map(VectorInputNativeArgument(_))))
          .zip(outputArguments.map(_.map(VectorOutputNativeArgument(_))))
          .flatMap { case ((a, b), c) =>
            a.orElse(b).orElse(c)
          }
      )
    }

    def callFunctionWrapped(name: String, arguments: List[NativeArgument]): Unit =
      arrowNativeInterfaceNumeric.callFunctionGen(
        name,
        scalarInputs = arguments.map {
          case ScalarInputNativeArgument(scalarInput) => Some(scalarInput)
          case _                                      => None
        },
        inputArguments = arguments.map {
          case VectorInputNativeArgument(vector) => Some(vector)
          case _                                 => None
        },
        outputArguments = arguments.map {
          case VectorOutputNativeArgument(vector) => Some(vector)
          case _                                  => None
        }
      )
    final def callFunction(
      name: String,
      inputArguments: List[Option[InputVectorWrapper]],
      outputArguments: List[Option[OutputVectorWrapper]]
    ): Unit = arrowNativeInterfaceNumeric.callFunctionGen(
      name = name,
      scalarInputs = List.fill(Math.max(inputArguments.size, outputArguments.size))(None),
      inputArguments = inputArguments,
      outputArguments = outputArguments
    )

  }

  object Float8VectorWrapper {
    def apply[R](float8Vector: Float8Vector)(implicit f: Float8Vector => R): R =
      f(float8Vector)

    implicit def float8ToOutput(float8Vector: Float8Vector): OutputVectorWrapper =
      OutputVectorWrapper.Float8VectorOutputWrapper(float8Vector)
    implicit def float8ToInput(float8Vector: Float8Vector): InputVectorWrapper =
      InputVectorWrapper.Float8VectorInputWrapper(float8Vector)
  }
  sealed trait NativeArgument
  object NativeArgument {
    sealed trait InputNativeArgument extends NativeArgument
    final case class ScalarInputNativeArgument(scalarInput: ScalarInput) extends NativeArgument
    sealed trait VectorNativeArgument extends NativeArgument
    final case class VectorInputNativeArgument(
      wrapped: VectorInputNativeArgument.InputVectorWrapper
    ) extends VectorNativeArgument
    object VectorInputNativeArgument {
      sealed trait InputVectorWrapper
      object InputVectorWrapper {
        final case class StringInputWrapper(string: String) extends InputVectorWrapper
        sealed trait InputArrowVectorWrapper extends InputVectorWrapper {
          def valueVector: ValueVector
        }
        final case class VarCharVectorInputWrapper(varCharVector: VarCharVector)
          extends InputArrowVectorWrapper {
          override def valueVector: ValueVector = varCharVector
        }
        final case class ByteBufferInputWrapper(byteBuffer: ByteBuffer, size: Int)
          extends InputVectorWrapper
        final case class Float8VectorInputWrapper(float8Vector: Float8Vector)
          extends InputArrowVectorWrapper {
          override def valueVector: ValueVector = float8Vector
        }
        final case class IntVectorInputWrapper(intVector: IntVector)
          extends InputArrowVectorWrapper {
          override def valueVector: ValueVector = intVector
        }
        final case class DateDayVectorInputWrapper(dateDayVector: DateDayVector)
          extends InputArrowVectorWrapper {
          override def valueVector: ValueVector = dateDayVector
        }
        final case class BigIntVectorInputWrapper(bigIntVector: BigIntVector)
          extends InputArrowVectorWrapper {
          override def valueVector: ValueVector = bigIntVector
        }
        final case class SmallIntVectorInputWrapper(smallIntVector: SmallIntVector)
          extends InputArrowVectorWrapper {
          override def valueVector: ValueVector = smallIntVector
        }
        final case class BitVectorInputWrapper(bitVector: BitVector)
          extends InputArrowVectorWrapper {
          override def valueVector: ValueVector = bitVector
        }
      }
    }
    final case class VectorOutputNativeArgument(
      wrapped: VectorOutputNativeArgument.OutputVectorWrapper
    ) extends VectorNativeArgument
    object VectorOutputNativeArgument {
      sealed trait OutputVectorWrapper {
        def valueVector: ValueVector
      }
      object OutputVectorWrapper {
        final case class Float8VectorOutputWrapper(float8Vector: Float8Vector)
          extends OutputVectorWrapper {
          override def valueVector: ValueVector = float8Vector
        }
        final case class IntVectorOutputWrapper(intVector: IntVector) extends OutputVectorWrapper {
          override def valueVector: ValueVector = intVector
        }
        final case class BigIntVectorOutputWrapper(bigIntVector: BigIntVector)
          extends OutputVectorWrapper {
          override def valueVector: ValueVector = bigIntVector
        }
        final case class VarCharVectorOutputWrapper(varCharVector: VarCharVector)
          extends OutputVectorWrapper {
          override def valueVector: ValueVector = varCharVector
        }
        final case class SmallIntVectorOutputWrapper(smallIntVector: SmallIntVector)
          extends OutputVectorWrapper {
          override def valueVector: ValueVector = smallIntVector
        }
        final case class BitVectorOutputWrapper(bitVector: BitVector)
          extends OutputVectorWrapper {
          override def valueVector: ValueVector = bitVector
        }
      }
    }

    def input(valueVector: ValueVector): VectorInputNativeArgument = VectorInputNativeArgument(
      SupportedVectorWrapper.wrapInput(valueVector)
    )
    def output(valueVector: ValueVector): VectorOutputNativeArgument = VectorOutputNativeArgument(
      SupportedVectorWrapper.wrapOutput(valueVector)
    )
    def scalar(string: String): VectorInputNativeArgument = VectorInputNativeArgument(
      StringInputWrapper(string)
    )
  }

  sealed trait ScalarInput extends Serializable
  object ScalarInput {
    def apply(int: Int): ScalarInput = ForInt(int)
    case class ForInt(value: Int) extends ScalarInput
  }
  object SupportedVectorWrapper {

    def wrapInput(valueVector: ValueVector): InputVectorWrapper =
      valueVector match {
        case intVector: IntVector => InputVectorWrapper.IntVectorInputWrapper(intVector)
        case dateDayVector: DateDayVector =>
          InputVectorWrapper.DateDayVectorInputWrapper(dateDayVector)
        case float8Vector: Float8Vector => InputVectorWrapper.Float8VectorInputWrapper(float8Vector)
        case varCharVector: VarCharVector =>
          InputVectorWrapper.VarCharVectorInputWrapper(varCharVector)
        case bigIntVector: BigIntVector => InputVectorWrapper.BigIntVectorInputWrapper(bigIntVector)
        case smallIntVector: SmallIntVector => InputVectorWrapper.SmallIntVectorInputWrapper(smallIntVector)
        case bitVector: BitVector => InputVectorWrapper.BitVectorInputWrapper(bitVector)
      }
    def wrapOutput(valueVector: ValueVector): OutputVectorWrapper = {
      valueVector match {
        case bigIntVector: BigIntVector =>
          OutputVectorWrapper.BigIntVectorOutputWrapper(bigIntVector)
        case intVector: IntVector => OutputVectorWrapper.IntVectorOutputWrapper(intVector)
        case float8Vector: Float8Vector =>
          OutputVectorWrapper.Float8VectorOutputWrapper(float8Vector)
        case varCharVector: VarCharVector =>
          OutputVectorWrapper.VarCharVectorOutputWrapper(varCharVector)
        case smallIntVector: SmallIntVector =>
          OutputVectorWrapper.SmallIntVectorOutputWrapper(smallIntVector)
        case bitVector: BitVector =>
          OutputVectorWrapper.BitVectorOutputWrapper(bitVector)
      }
    }
  }

  final case class DeferredArrowInterface(subInterface: () => ArrowNativeInterface)
    extends ArrowNativeInterface {
    def callFunctionWrapped(name: String, arguments: List[NativeArgument]): Unit =
      subInterface().callFunctionWrapped(name, arguments)
  }
}
