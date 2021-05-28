package com.nec.arrow

import java.nio.file.Path
import com.nec.arrow.ArrowTransferStructures.non_null_double_vector
import com.nec.arrow.ArrowInterfaces.c_double_vector
import com.nec.arrow.ArrowInterfaces.non_null_double_vector_to_float8Vector
import com.sun.jna.Library
import org.apache.arrow.vector.Float8Vector

final class CArrowNativeInterfaceNumeric(libPath: Path) extends ArrowNativeInterfaceNumeric {
  override def callFunction(
    name: String,
    inputArguments: List[Option[Float8Vector]],
    outputArguments: List[Option[Float8Vector]]
  ): Unit = CArrowNativeInterfaceNumeric.executeC(
    libPath = libPath,
    functionName = name,
    inputArguments = inputArguments,
    outputArguments = outputArguments
  )
}

object CArrowNativeInterfaceNumeric {

  private def executeC(
    libPath: Path,
    functionName: String,
    inputArguments: List[Option[Float8Vector]],
    outputArguments: List[Option[Float8Vector]]
  ): Unit = {
    import scala.collection.JavaConverters._
    val nativeLibraryHandler =
      new Library.Handler(libPath.toString, classOf[Library], Map.empty[String, Any].asJava)
    val nl = nativeLibraryHandler.getNativeLibrary
    val fn = nl.getFunction(functionName)

    val outputStructs = outputArguments.map(_.map(doubleVector => {
      new non_null_double_vector(doubleVector.getValueCount)
    }))

    val invokeArgs: Array[java.lang.Object] = inputArguments
      .zip(outputStructs)
      .map {
        case ((Some(vcv), _)) =>
          c_double_vector(vcv)
        case ((_, Some(structVector))) =>
          structVector
        case other =>
          sys.error(s"Unexpected state: $other")
      }
      .toArray

    fn.invokeLong(invokeArgs)

    outputStructs.zip(outputArguments).foreach {
      case (Some(struct), Some(vec)) =>
        non_null_double_vector_to_float8Vector(struct, vec)
      case _ =>
    }
  }
}
