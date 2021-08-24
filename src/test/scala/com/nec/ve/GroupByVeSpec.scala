package com.nec.ve

import com.nec.arrow.WithTestAllocator

import java.nio.file.Paths
import java.time.Instant
import com.nec.arrow.functions.GroupBy
import com.nec.arrow.functions.GroupBy.groupJVM
import com.nec.arrow.ArrowVectorBuilders
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.VeArrowNativeInterface
import com.nec.aurora.Aurora
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.IntVector
import org.scalatest.freespec.AnyFreeSpec

final class GroupByVeSpec extends AnyFreeSpec {
  "We can group by column" in {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val oPath = VeKernelCompiler("avg", veBuildPath).compile_c(
      List(TransferDefinitions.TransferDefinitionsSourceCode, GroupBy.GroupBySourceCode)
        .mkString("\n\n")
    )
    val proc = Aurora.veo_proc_create(0)
    val (sorted, expectedSorted) =
      try {
        val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(proc)
        try {
          WithTestAllocator { alloc =>
            val outValuesVector = new Float8Vector("values", alloc)
            val outGroupsVector = new Float8Vector("groups", alloc)
            val outCountVector = new Float8Vector("groupCounts", alloc)

            try {
              val groupingColumn: Seq[Double] =
                Seq(5, 20, 40, 100, 5, 20, 40, 91, 100, 100, 100, 100)
              val valuesColumn: Seq[Double] =
                Seq(10, 55, 41, 84, 43, 23, 44, 55, 109, 101, 102, 103)

              val lib: Long = Aurora.veo_load_library(proc, oPath.toString)
              ArrowVectorBuilders.withDirectFloat8Vector(groupingColumn) { groupingColumnVec =>
                ArrowVectorBuilders.withDirectFloat8Vector(valuesColumn) { valuesColumnVec =>
                  GroupBy.runOn(new VeArrowNativeInterface(proc, lib))(
                    groupingColumnVec,
                    valuesColumnVec,
                    outGroupsVector,
                    outCountVector,
                    outValuesVector
                  )

                  val groups = (0 to outGroupsVector.getValueCount - 1).map { i =>
                    outGroupsVector.get(i)
                  }.toList
                  val groupCounts = (0 to outCountVector.getValueCount - 1).map { i =>
                    outCountVector.get(i).toInt
                  }.toList
                  val values = (0 to outValuesVector.getValueCount - 1).map { i =>
                    outValuesVector.get(i)
                  }.toList

                  var valueStart = 0
                  var result = scala.collection.mutable.Map[Double, List[Double]]()
                  groups.zipWithIndex.foreach { case (group, index) =>
                    val groupValues = values.slice(valueStart, valueStart + groupCounts(index))
                    valueStart += groupCounts(index)
                    result(group) = groupValues
                  }

                  (result, groupJVM(groupingColumnVec, valuesColumnVec))
                }
              }
            } finally {
              outValuesVector.close()
              outGroupsVector.close()
              outCountVector.close()
            }
          }
        } finally Aurora.veo_context_close(ctx)
      } finally Aurora.veo_proc_destroy(proc)
    assert(sorted.nonEmpty)
    assert(sorted == expectedSorted)
  }

}
