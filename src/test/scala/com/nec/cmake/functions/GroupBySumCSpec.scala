package com.nec.cmake.functions

import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant
import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.WithTestAllocator
import com.nec.arrow.functions.GroupBySum._
import com.nec.arrow.ArrowVectorBuilders
import com.nec.arrow.CArrowNativeInterfaceNumeric
import com.nec.cmake.CMakeBuilder
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

final class GroupBySumCSpec extends AnyFreeSpec {

  "Through Arrow, it works" in {
    val veBuildPath = Paths.get("target", "c", s"${Instant.now().toEpochMilli}").toAbsolutePath
    Files.createDirectory(veBuildPath)

    val soPath = CMakeBuilder.buildC(
      List(TransferDefinitionsSourceCode, "\n\n", GroupBySumSourceCode)
        .mkString("\n\n")
    )

    WithTestAllocator { alloc =>
      val outGroupsVector = new Float8Vector("groups", alloc)
      val outValuesVector = new Float8Vector("values", alloc)

      val groupingColumn: Seq[Double] = Seq(5, 20, 40, 100, 5, 20, 40, 91, 100)
      val valuesColumn: Seq[Double] = Seq(10, 55, 41, 84, 43, 23, 44, 55, 109)

      try ArrowVectorBuilders.withDirectFloat8Vector(groupingColumn) { groupingColumnVec =>
        ArrowVectorBuilders.withDirectFloat8Vector(valuesColumn) { valuesColumnVec =>
          runOn(new CArrowNativeInterfaceNumeric(soPath.toString))(
            groupingColumnVec,
            valuesColumnVec,
            outGroupsVector,
            outValuesVector
          )
          val result = (0 until outGroupsVector.getValueCount)
            .map(idx => (outGroupsVector.get(idx), outValuesVector.get(idx)))

          assert(result.nonEmpty)
          assert(result.toMap == groupBySumJVM(groupingColumnVec, valuesColumnVec))
        }
      } finally {
        outGroupsVector.close()
        outValuesVector.close()
      }
    }
  }
}
