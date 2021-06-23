package com.nec.cmake.functions

import java.nio.file.{Files, Paths}
import java.time.Instant

import com.nec.arrow.ArrowVectorBuilders.{withArrowFloat8Vector, withDirectFloat8Vector}
import com.nec.arrow.{CArrowNativeInterfaceNumeric, TransferDefinitions}
import com.nec.arrow.functions.{Sort, Sum}
import com.nec.cmake.CMakeBuilder
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

final class SortCSpec extends AnyFreeSpec {

  "Through Arrow, it works" in {
    val input: Seq[Double] = Seq(500.0, 200.0, 1.0, 280.0, 1000.0)
    val veBuildPath = Paths.get("target", "c", s"${Instant.now().toEpochMilli}").toAbsolutePath
    Files.createDirectory(veBuildPath)
    val soPath = veBuildPath.resolve("sort.so")
    val theCommand = List(
      "g++",
      "-std=c++11",
      "-fpic",
      "-o",
      soPath.toString,
      "-I./src/main/resources/com/nec/arrow/functions/cpp",
      "-shared",
      "-I./src/main/resources/com/nec/arrow/functions",
      "-I./src/main/resources/com/nec/arrow/",
      "./src/main/resources/com/nec/arrow/functions/cpp/sorter.cc",

    )

    import scala.sys.process._
    info(theCommand.!!)

    withDirectFloat8Vector(input) { vector =>
      val alloc = new RootAllocator(Integer.MAX_VALUE)
      val outVector = new Float8Vector("value", alloc)
      Sort.runOn(new CArrowNativeInterfaceNumeric(soPath.toString))(vector, outVector)
      val outData = (0 until outVector.getValueCount).map(idx => outVector.get(idx))
      assert(
         outData == Sort.sortJVM(vector)
      )
    }
  }

}
