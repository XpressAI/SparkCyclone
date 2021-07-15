package com.nec.cmake.functions

import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant
import com.nec.arrow.ArrowVectorBuilders.withDirectFloat8Vector
import com.nec.arrow.CArrowNativeInterfaceNumeric
import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.WithTestAllocator
import com.nec.arrow.functions.Sort
import com.nec.cmake.CMakeBuilder
import com.nec.cmake.functions.SortCSpec.SorterSource
import com.nec.spark.agile.CppResource
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

object SortCSpec {
  val SorterSource: String = CppResource("cpp/sorter.cc").readString
}

final class SortCSpec extends AnyFreeSpec {
  "Through Arrow, it works" in {
    val input: Seq[Double] = Seq(500.0, 200.0, 1.0, 280.0, 1000.0)
    val veBuildPath = Paths.get("target", "c", s"${Instant.now().toEpochMilli}").toAbsolutePath
    Files.createDirectory(veBuildPath)

    val cLib = CMakeBuilder.buildC(
      List(TransferDefinitionsSourceCode, SorterSource)
        .mkString("\n\n")
    )

    withDirectFloat8Vector(input) { vector =>
      WithTestAllocator { alloc =>
        val outVector = new Float8Vector("value", alloc)
        Sort.runOn(new CArrowNativeInterfaceNumeric(cLib.toString))(vector, outVector)
        val outData = (0 until outVector.getValueCount).map(idx => outVector.get(idx))
        assert(outData == Sort.sortJVM(vector))
      }
    }
  }

}
