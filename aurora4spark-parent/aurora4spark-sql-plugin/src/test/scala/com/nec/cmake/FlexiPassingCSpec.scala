package com.nec.cmake
import com.nec.arrow.ArrowInterfaces.c_double_vector
import com.nec.arrow.ArrowInterfaces.non_null_double_vector_to_float8Vector
import com.nec.arrow.ArrowTransferStructures.non_null_double_vector
import com.nec.arrow.ArrowVectorBuilders
import com.nec.arrow.TransferDefinitions
import com.nec.ve.FlexiPassingVESpec
import com.nec.ve.FlexiPassingVESpec.Add1Mul2
import com.nec.ve.FlexiPassingVESpec.InVh
import com.nec.ve.FlexiPassingVESpec.NativeIf
import com.nec.ve.FlexiPassingVESpec.RichFloat8
import com.sun.jna.Library
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

object FlexiPassingCSpec {}
final class FlexiPassingCSpec extends AnyFreeSpec {
  "Through Arrow, it works" in {
    val libPath = CMakeBuilder.buildC(
      List(TransferDefinitions.TransferDefinitionsSourceCode, FlexiPassingVESpec.Sources)
        .mkString("\n\n")
    )
    val nativeIf: NativeIf[InVh] = new NativeIf[InVh] {
      override def call(functionName: String, input: InVh[Float8Vector]): InVh[Float8Vector] = {
        import scala.collection.JavaConverters._
        val nativeLibraryHandler =
          new Library.Handler(libPath.toString, classOf[Library], Map.empty[String, Any].asJava)
        val nl = nativeLibraryHandler.getNativeLibrary
        val fn = nl.getFunction(functionName)
        val outputStruct = new non_null_double_vector()
        val invokeArgs: Array[AnyRef] = List(c_double_vector(input.contents), outputStruct).toArray
        fn.invokeLong(invokeArgs)
        val ra = new RootAllocator()
        val outputVector = new Float8Vector("result", ra)
        non_null_double_vector_to_float8Vector(input = outputStruct, output = outputVector)
        InVh(outputVector)
      }
    }
    val result = ArrowVectorBuilders.withDirectFloat8Vector(List(1, 2, 3)) { vcv =>
      Add1Mul2.call(nativeIf)(InVh(vcv)).contents.toList
    }
    assert(result == List[Double](4, 6, 8))
  }
}
