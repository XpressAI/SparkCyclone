package com.nec.arrow

import com.nec.arrow.ArrowInterfaces.c_bounded_data
import com.nec.arrow.ArrowTransferStructures._
import com.nec.arrow.ArrowInterfaces._
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.ByteBufferInputWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.Float8VectorInputWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.IntVectorInputWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.StringInputWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.VarCharVectorInputWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorOutputNativeArgument.OutputVectorWrapper.BigIntVectorOutputWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorOutputNativeArgument.OutputVectorWrapper.Float8VectorOutputWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorOutputNativeArgument.OutputVectorWrapper.IntVectorOutputWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorOutputNativeArgument.OutputVectorWrapper.VarCharVectorOutputWrapper
import com.sun.jna.Library
import com.nec.arrow.ArrowNativeInterface._
import com.nec.arrow.ArrowNativeInterface.SupportedVectorWrapper._
import com.typesafe.scalalogging.LazyLogging

final class CArrowNativeInterface(libPath: String) extends ArrowNativeInterface {
  override def callFunctionWrapped(name: String, arguments: List[NativeArgument]): Unit =
    CArrowNativeInterface.executeC(libPath = libPath, functionName = name, arguments = arguments)
}

object CArrowNativeInterface extends LazyLogging {

  private def executeC(libPath: String, functionName: String, arguments: List[NativeArgument]): Unit = {
    import scala.collection.JavaConverters._
    val nativeLibraryHandler =
      new Library.Handler(libPath, classOf[Library], Map.empty[String, Any].asJava)
    val nl = nativeLibraryHandler.getNativeLibrary
    val fn = nl.getFunction(functionName)
    logger.debug(s"Arguments are = $arguments")

    val vectorExtractions = scala.collection.mutable.Buffer.empty[() => Unit]

    val invokeArgs: Array[java.lang.Object] = arguments.map {
      case NativeArgument.ScalarInputNativeArgument(ScalarInput.ForInt(int)) =>
        java.lang.Integer.valueOf(int)
      case NativeArgument.VectorInputNativeArgument(ByteBufferInputWrapper(buffer, size)) =>
        c_bounded_data(buffer, size)
      case NativeArgument.VectorInputNativeArgument(StringInputWrapper(str)) =>
        c_bounded_string(str)
      case NativeArgument.VectorInputNativeArgument(Float8VectorInputWrapper(vcv)) =>
        c_nullable_double_vector(vcv)
      case NativeArgument.VectorInputNativeArgument(IntVectorInputWrapper(vcv)) =>
        c_int2_vector(vcv)
      case NativeArgument.VectorInputNativeArgument(VarCharVectorInputWrapper(vcv)) =>
        c_non_null_varchar_vector(vcv)
      case NativeArgument.VectorOutputNativeArgument(Float8VectorOutputWrapper(doubleVector)) =>
        val struct = new nullable_double_vector()
        vectorExtractions.append(() => nullable_double_vector_to_float8Vector(struct, doubleVector))
        struct
      case NativeArgument.VectorOutputNativeArgument(IntVectorOutputWrapper(intVector)) =>
        val struct = new non_null_int_vector()
        vectorExtractions.append(() => non_null_int_vector_to_IntVector(struct, intVector))
        struct
      case NativeArgument.VectorOutputNativeArgument(BigIntVectorOutputWrapper(bigIntVector)) =>
        val struct = new nullable_bigint_vector()
        vectorExtractions.append(() => nullable_bigint_vector_to_bigintVector(struct, bigIntVector))
        struct
      case NativeArgument.VectorOutputNativeArgument(VarCharVectorOutputWrapper(vec)) =>
        val struct = new non_null_varchar_vector()
        vectorExtractions.append(() => non_null_varchar_vector_to_VarCharVector(struct, vec))
        struct
      case other =>
        throw new MatchError(s"Unmatched for input: ${other}")
    }.toArray

    def ia: String = invokeArgs.mkString("[", ",", "]")

    logger.debug(s"Invoke args are => $ia (size ${invokeArgs.length})")

    fn.invokeLong(invokeArgs)
    vectorExtractions.foreach(_.apply())

    logger.debug(s"Result of invoke args => $ia (size ${invokeArgs.length})")
  }
}
