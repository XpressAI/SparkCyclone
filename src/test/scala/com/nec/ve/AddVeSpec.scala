package com.nec.ve

import java.nio.file.Paths
import java.time.Instant
import com.nec.arrow.functions.AddPairwise.addJVM
import com.nec.arrow.functions.AddPairwise.runOn
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.VeArrowNativeInterfaceNumeric
import com.nec.arrow.ArrowVectorBuilders.withArrowFloat8Vector
import com.nec.arrow.WithTestAllocator
import com.nec.arrow.functions.AddPairwise
import com.nec.aurora.Aurora
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

final class AddVeSpec extends AnyFreeSpec {

  "We can get an addition result back" in {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val libPath = VeKernelCompiler("add", veBuildPath).compile_c(
      List(TransferDefinitions.TransferDefinitionsSourceCode, AddPairwise.PairwiseSumCode)
        .mkString("\n\n")
    )
    val proc = Aurora.veo_proc_create(0)
    val (addResult, expectedAddResult) =
      try {
        val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(proc)
        try {
          val firstColumn: Seq[Seq[Double]] = Seq(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
          val secondColumn: Seq[Seq[Double]] = Seq(Seq(10, 20, 30, 40, 50, 60, 70, 80, 90, 100))

          val lib: Long = Aurora.veo_load_library(proc, libPath.toString)
          WithTestAllocator { alloc =>
            withArrowFloat8Vector(firstColumn) { firstVector =>
              withArrowFloat8Vector(secondColumn) { secondVector =>
                val outVector = new Float8Vector("value", alloc)
                try {
                  runOn(new VeArrowNativeInterfaceNumeric(proc, ctx, lib))(
                    firstVector,
                    secondVector,
                    outVector
                  )
                  val pairwiseSum = (0 until outVector.getValueCount).map(outVector.get).toList

                  (pairwiseSum, addJVM(firstVector, secondVector))
                } finally outVector.close()
              }
            }
          }
        } finally Aurora.veo_context_close(ctx)
      } finally Aurora.veo_proc_destroy(proc)

    assert(addResult.nonEmpty)
    assert(addResult == expectedAddResult)
  }
}
