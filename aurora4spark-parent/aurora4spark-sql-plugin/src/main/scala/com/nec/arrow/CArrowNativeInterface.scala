package com.nec.arrow

import com.nec.arrow.ArrowTransferStructures.non_null_int_vector
import com.nec.arrow.ArrowInterfaces.c_varchar_vector
import com.nec.arrow.ArrowInterfaces.non_null_int_vector_to_IntVector
import com.sun.jna.Library
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.VarCharVector

import java.nio.file.Path

final class CArrowNativeInterface(libPath: Path) extends ArrowNativeInterface {
  override def callFunction(
    name: String,
    inputArguments: List[Option[VarCharVector]],
    outputArguments: List[Option[IntVector]]
  ): Unit = CArrowNativeInterface.executeC(
    libPath = libPath,
    functionName = name,
    inputArguments = inputArguments,
    outputArguments = outputArguments
  )
}

object CArrowNativeInterface {

  private def executeC(
    libPath: Path,
    functionName: String,
    inputArguments: List[Option[VarCharVector]],
    outputArguments: List[Option[IntVector]]
  ): Unit = {
    import scala.collection.JavaConverters._
    val nativeLibraryHandler =
      new Library.Handler(libPath.toString, classOf[Library], Map.empty[String, Any].asJava)
    val nl = nativeLibraryHandler.getNativeLibrary
    val fn = nl.getFunction(functionName)

    val outputStructs = outputArguments.map(_.map(intVector => new non_null_int_vector()))

    val invokeArgs: Array[AnyRef] = inputArguments
      .zip(outputStructs)
      .map {
        case ((Some(vcv), _)) =>
          c_varchar_vector(vcv).asInstanceOf[AnyRef]
        case ((_, Some(structIntVector))) =>
          structIntVector
        case other =>
          sys.error(s"Unexpected state: $other")
      }
      .toArray

    fn.invokeLong(invokeArgs)

    outputStructs.zip(outputArguments).foreach {
      case (Some(struct), Some(vec)) =>
        non_null_int_vector_to_IntVector(struct, vec)
      case _ =>
    }
  }
}
