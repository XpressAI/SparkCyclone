package com.nec.arrow

import com.nec.arrow.ArrowInterfaces.c_bounded_data
import com.nec.arrow.ArrowTransferStructures.{non_null_double_vector, non_null_int_vector}
import com.nec.arrow.ArrowInterfaces.{c_bounded_string, c_double_vector, c_int2_vector, non_null_double_vector_to_float8Vector, non_null_int_vector_to_IntVector}
import com.sun.jna.Library
import com.nec.arrow.ArrowNativeInterfaceNumeric._
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper._

final class CArrowNativeInterfaceNumeric(libPath: String) extends ArrowNativeInterfaceNumeric {
  override def callFunctionGen(
    name: String,
    inputArguments: List[Option[SupportedVectorWrapper]],
    outputArguments: List[Option[SupportedVectorWrapper]]
  ): Unit = CArrowNativeInterfaceNumeric.executeC(
    libPath = libPath,
    functionName = name,
    inputArguments = inputArguments,
    outputArguments = outputArguments
  )
}

object CArrowNativeInterfaceNumeric {

  private def executeC(
    libPath: String,
    functionName: String,
    inputArguments: List[Option[SupportedVectorWrapper]],
    outputArguments: List[Option[SupportedVectorWrapper]]
  ): Unit = {
    import scala.collection.JavaConverters._
    val nativeLibraryHandler =
      new Library.Handler(libPath, classOf[Library], Map.empty[String, Any].asJava)
    val nl = nativeLibraryHandler.getNativeLibrary
    val fn = nl.getFunction(functionName)

    val outputStructs = outputArguments.map(_.map {
      case Float8VectorWrapper(doubleVector) => new non_null_double_vector(doubleVector.getValueCount)
      case IntVectorWrapper(intVector) => new non_null_int_vector()
      case other => throw new MatchError(s"Not supported for output: ${other}")

    })

    val invokeArgs: Array[java.lang.Object] = inputArguments
      .zip(outputStructs)
      .map {
        case ((Some(ByteBufferWrapper(buffer, size)), _)) =>
          c_bounded_data(buffer, size)
        case ((Some(StringWrapper(str)), _)) =>
          c_bounded_string(str)
        case ((Some(Float8VectorWrapper(vcv)), _)) =>
          c_double_vector(vcv)
        case ((Some(IntVectorWrapper(vcv)), _)) =>
          c_int2_vector(vcv)
        case ((_, Some(structVector))) =>
          structVector
        case other =>
          throw new MatchError(s"Unmatched for input: ${other}")
      }
      .toArray

    fn.invokeLong(invokeArgs)

    outputStructs.zip(outputArguments).foreach {
      case (Some(struct), Some(Float8VectorWrapper(vec))) =>
        non_null_double_vector_to_float8Vector(struct.asInstanceOf[non_null_double_vector], vec)
      case (Some(struct), Some(IntVectorWrapper(vec))) =>
        non_null_int_vector_to_IntVector(struct.asInstanceOf[non_null_int_vector], vec)
      case _ =>
    }

  }
}
