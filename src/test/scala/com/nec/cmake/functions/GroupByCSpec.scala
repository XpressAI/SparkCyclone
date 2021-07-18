package com.nec.cmake.functions

import com.eed3si9n.expecty.Expecty.expect

import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant
import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.WithTestAllocator
import com.nec.arrow.functions.GroupBy._
import com.nec.arrow.ArrowVectorBuilders
import com.nec.arrow.CArrowNativeInterfaceNumeric
import com.nec.cmake.CMakeBuilder
import com.nec.cmake.functions.ParseCSVSpec.RichFloat8
import com.nec.cmake.functions.ParseCSVSpec.RichIntVector
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

final class GroupByCSpec extends AnyFreeSpec {

  "Through Arrow, it works" in {
    val veBuildPath = Paths.get("target", "c", s"${Instant.now().toEpochMilli}").toAbsolutePath
    Files.createDirectory(veBuildPath)

    val soPath = CMakeBuilder.buildC(
      List(TransferDefinitionsSourceCode, "\n\n", GroupBySourceCode)
        .mkString("\n\n")
    )

    WithTestAllocator { alloc =>
      val outGroupsVector = new Float8Vector("groups", alloc)
      val outValuesVector = new Float8Vector("values", alloc)
      val outCountVector = new Float8Vector("count", alloc)

      val groupingColumn: Seq[Double] = Seq(5, 20, 40, 100, 5, 20, 40, 91, 100)
      val valuesColumn: Seq[Double] = Seq(10, 55, 41, 84, 43, 23, 44, 55, 109)

      try ArrowVectorBuilders.withDirectFloat8Vector(groupingColumn) { groupingColumnVec =>
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

          val values = counts.zipWithIndex.foldLeft((Seq.empty[Seq[Double]], 0L)) {
            case (state, (value, idx)) =>
              val totalCountSoFar = state._2
              val elemsSoFar = state._1
              val valz = (totalCountSoFar until (totalCountSoFar + value).toInt).map(index =>
                outValuesVector.get(index.toInt)
              )
              (elemsSoFar :+ valz, totalCountSoFar + valz.size)
          }
          val groupKeys =
            (0 until outGroupsVector.getValueCount).map(idx => outGroupsVector.get(idx))

          val result = groupKeys.zip(values._1)
          assert(result.nonEmpty)
          val resultMap = result.toMap
          expect(
            resultMap == Map[Double, Seq[Double]](
              (5: Double) -> Seq[Double](10, 43),
              (20: Double) -> Seq[Double](55, 23),
              (40: Double) -> Seq[Double](41, 44),
              (91: Double) -> Seq[Double](55),
              (100: Double) -> Seq[Double](84, 109)
            )
          )
        }
      } finally {
        outGroupsVector.close()
        outValuesVector.close()
        outCountVector.close()
      }
    }
  }
}
