package com.nec.cmake.functions

import java.nio.file.{Files, Paths}
import java.time.Instant

import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.functions.GroupBy._
import com.nec.arrow.{ArrowVectorBuilders, CArrowNativeInterfaceNumeric}
import com.nec.cmake.CMakeBuilder
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{Float8Vector, IntVector}
import org.scalatest.freespec.AnyFreeSpec

final class GroupByCSpec extends AnyFreeSpec {

  "Through Arrow, it works" in {
    val veBuildPath = Paths.get("target", "c", s"${Instant.now().toEpochMilli}").toAbsolutePath
    Files.createDirectory(veBuildPath)

    val soPath = CMakeBuilder.buildC(
      List(TransferDefinitionsSourceCode, "\n\n", GroupBySourceCode)
        .mkString("\n\n")
    )

    val alloc = new RootAllocator(Integer.MAX_VALUE)
    val outGroupsVector = new Float8Vector("groups", alloc)
    val outValuesVector = new Float8Vector("values", alloc)
    val outCountVector = new IntVector("count", alloc)

    val groupingColumn: Seq[Double] = Seq(5, 20, 40, 100, 5, 20, 40, 91, 100)
    val valuesColumn: Seq[Double] = Seq(10, 55, 41, 84, 43, 23 , 44, 55, 109)

    ArrowVectorBuilders.withDirectFloat8Vector(groupingColumn) { groupingColumnVec =>
      ArrowVectorBuilders.withDirectFloat8Vector(valuesColumn) { valuesColumnVec =>
        runOn(new CArrowNativeInterfaceNumeric(soPath.toString))(
          groupingColumnVec,
          valuesColumnVec,
          outGroupsVector,
          outCountVector,
          outValuesVector
        )
        val counts = (0 until outCountVector.getValueCount)
          .map(i => outCountVector.get(i))
          .toList

        var values = (0 until outValuesVector.getValueCount)
          .map(i => outValuesVector.get(i))
          .toList

        val result = counts.zipWithIndex.map{
          case (value, idx) => {
            val key = outGroupsVector.get(idx)
            val valz = values.take(value)
            values = values.drop(value)
            (key, valz.toSeq)
          }
        }.toMap

        (result, groupJVM(groupingColumnVec, valuesColumnVec))
      }
    }
  }
}
