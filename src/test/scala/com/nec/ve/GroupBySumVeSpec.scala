package com.nec.ve

import com.nec.arrow.WithTestAllocator

import java.nio.file.Paths
import java.time.Instant
import com.nec.arrow.functions.GroupBySum
import com.nec.arrow.functions.GroupBySum._
import com.nec.arrow.ArrowVectorBuilders
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.VeArrowNativeInterfaceNumeric
import com.nec.aurora.Aurora
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

final class GroupBySumVeSpec extends AnyFreeSpec {
  "We can group by column" in {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val oPath = VeKernelCompiler("avg", veBuildPath).compile_c(
      List(TransferDefinitions.TransferDefinitionsSourceCode, GroupBySum.GroupBySumSourceCode)
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

            val groupingColumn: Seq[Double] = Seq(5, 20, 40, 100, 5, 20, 40, 91, 100)
            val valuesColumn: Seq[Double] = Seq(10, 55, 41, 84, 43, 23, 44, 55, 109)

            val lib: Long = Aurora.veo_load_library(proc, oPath.toString)
            try ArrowVectorBuilders.withDirectFloat8Vector(groupingColumn) { groupingColumnVec =>
              ArrowVectorBuilders.withDirectFloat8Vector(valuesColumn) { valuesColumnVec =>
                runOn(new VeArrowNativeInterfaceNumeric(proc, lib))(
                  groupingColumnVec,
                  valuesColumnVec,
                  outGroupsVector,
                  outValuesVector
                )

                val result = (0 until outGroupsVector.getValueCount)
                  .map(idx => (outGroupsVector.get(idx), outValuesVector.get(idx)))

                (result.toMap, groupBySumJVM(groupingColumnVec, valuesColumnVec))
              }
            } finally {
              outValuesVector.close()
              outGroupsVector.close()
            }
          }
        } finally Aurora.veo_context_close(ctx)
      } finally Aurora.veo_proc_destroy(proc)
    assert(sorted.nonEmpty)
    assert(sorted == expectedSorted)
  }

}
