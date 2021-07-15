package com.nec.cmake.functions

import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant
import com.nec.arrow.ArrowVectorBuilders
import com.nec.arrow.CArrowNativeInterfaceNumeric
import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.WithTestAllocator
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec
import com.nec.arrow.functions.Join._
import com.nec.cmake.CMakeBuilder
import com.nec.cmake.functions.JoinCSpec.JoinerSource
import com.nec.spark.agile.CppResource

object JoinCSpec {
  val JoinerSource: String = CppResource("cpp/joiner.cc").readString
}

final class JoinCSpec extends AnyFreeSpec {

  "Through Arrow, it works" in {
    val veBuildPath = Paths.get("target", "c", s"${Instant.now().toEpochMilli}").toAbsolutePath
    Files.createDirectory(veBuildPath)

    val soPath = CMakeBuilder.buildC(
      List(TransferDefinitionsSourceCode, "\n\n", JoinerSource)
        .mkString("\n\n")
    )

    WithTestAllocator { alloc =>
      val outVector = new Float8Vector("value", alloc)
      val firstColumn: Seq[Double] = Seq(5, 1, 2, 34, 6)
      val secondColumn: Seq[Double] = Seq(100, 15, 92, 331, 49)
      val firstColumnKeys: Seq[Int] = Seq(1, 2, 3, 4, 5)
      val secondColumnKeys: Seq[Int] = Seq(4, 2, 5, 200, 800)
      try ArrowVectorBuilders.withDirectFloat8Vector(firstColumn) { firstColumnVec =>
        ArrowVectorBuilders.withDirectFloat8Vector(secondColumn) { secondColumnVec =>
          ArrowVectorBuilders.withDirectIntVector(firstColumnKeys) { firstKeysVec =>
            ArrowVectorBuilders.withDirectIntVector(secondColumnKeys) { secondKeysVec =>
              runOn(new CArrowNativeInterfaceNumeric(soPath.toString))(
                firstColumnVec,
                secondColumnVec,
                firstKeysVec,
                secondKeysVec,
                outVector
              )
              val res = (0 until outVector.getValueCount)
                .map(i => outVector.get(i))
                .toList
                .splitAt(outVector.getValueCount / 2)
              val joinResult = res._1.zip(res._2)
              (joinResult, joinJVM(firstColumnVec, secondColumnVec, firstKeysVec, secondKeysVec))
            }
          }
        }
      } finally outVector.close()
    }
  }

}
