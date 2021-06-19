package com.nec.cmake.functions

import java.nio.file.{Files, Paths}
import java.time.Instant

import com.nec.arrow.ArrowVectorBuilders.withDirectFloat8Vector
import com.nec.arrow.{ArrowVectorBuilders, CArrowNativeInterfaceNumeric, VeArrowNativeInterfaceNumeric}
import com.nec.arrow.functions.Sort
import com.nec.ve.JoinVeSpec.Join.{joinJVM, runOn}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

final class JoinCSpec extends AnyFreeSpec {

  "Through Arrow, it works" in {
    val input: Seq[Double] = Seq(500.0, 200.0, 1.0, 280.0, 1000.0)
    val veBuildPath = Paths.get("target", "c", s"${Instant.now().toEpochMilli}").toAbsolutePath
    Files.createDirectory(veBuildPath)
    val soPath = veBuildPath.resolve("join.so")
    val theCommand = List(
      "g++",
      "-std=c++11",
      "-o",
      soPath.toString,
      "-I./src/main/resources/com/nec/arrow/functions/cpp",
      "-shared",
      "-I./src/main/resources/com/nec/arrow/functions",
      "-I./src/main/resources/com/nec/arrow/",
      "./src/main/resources/com/nec/arrow/functions/cpp/joiner.cc",

    )

    import scala.sys.process._
    info(theCommand.!!)
    val alloc = new RootAllocator(Integer.MAX_VALUE)
    val outVector = new Float8Vector("value", alloc)
    val firstColumn: Seq[Double] = Seq(5, 1, 2, 34, 6)
    val secondColumn: Seq[Double] = Seq(100, 15, 92, 331, 49)
    val firstColumnKeys: Seq[Int] = Seq(1, 2, 3, 4, 5)
    val secondColumnKeys: Seq[Int] = Seq(4, 2, 5, 200, 800)
    ArrowVectorBuilders.withDirectFloat8Vector(firstColumn) { firstColumnVec =>
      ArrowVectorBuilders.withDirectFloat8Vector(secondColumn){ secondColumnVec =>
        ArrowVectorBuilders.withDirectIntVector(firstColumnKeys) { firstKeysVec =>
          ArrowVectorBuilders.withDirectIntVector(secondColumnKeys) { secondKeysVec =>
            runOn(new CArrowNativeInterfaceNumeric(soPath.toString))(firstColumnVec,
              secondColumnVec, firstKeysVec, secondKeysVec, outVector)
            val res = (0 until outVector.getValueCount).map(i => outVector.get(i)).toList
              .splitAt(outVector.getValueCount/2)
            val joinResult = res._1.zip(res._2)
            (joinResult, joinJVM(firstColumnVec, secondColumnVec, firstKeysVec, secondKeysVec))
          }
        }
      }
    }
  }

}
