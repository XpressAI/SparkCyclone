package com.nec.ve

import com.nec.arrow.WithTestAllocator

import java.nio.file.Paths
import java.time.Instant
import com.nec.arrow.functions.GroupBy
import com.nec.arrow.functions.GroupBy.groupJVM
import com.nec.arrow.ArrowVectorBuilders
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.VeArrowNativeInterfaceNumeric
import com.nec.aurora.Aurora
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.IntVector
import org.scalatest.freespec.AnyFreeSpec

final class GroupByVeSpec extends AnyFreeSpec {
  "We can group by column" ignore {
    // TODO new failure for some reason
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
            val outCountVector = new IntVector("groupCounts", alloc)
            try {

              val groupingColumn: Seq[Double] = Seq(5, 20, 40, 100, 5, 20, 40, 91, 100)
              val valuesColumn: Seq[Double] = Seq(10, 55, 41, 84, 43, 23, 44, 55, 109)

              val lib: Long = Aurora.veo_load_library(proc, oPath.toString)
              ArrowVectorBuilders.withDirectFloat8Vector(groupingColumn) { groupingColumnVec =>
                ArrowVectorBuilders.withDirectFloat8Vector(valuesColumn) { valuesColumnVec =>
                  GroupBy.runOn(new VeArrowNativeInterfaceNumeric(proc, ctx, lib))(
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
                    case (state, (value, idx)) => {
                      val totalCountSoFar = state._2
                      val elemsSoFar = state._1
                      val valz = (totalCountSoFar until totalCountSoFar + value).map(index =>
                        outValuesVector.get(index.toInt)
                      )
                      (elemsSoFar :+ valz, totalCountSoFar + valz.size)
                    }
                  }
                  val groupKeys =
                    (0 until outGroupsVector.getValueCount).map(idx => outGroupsVector.get(idx))

                  val result = groupKeys.zip(values._1).toMap
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
