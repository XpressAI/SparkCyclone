package com.nec.ve

import com.nec.arrow.ArrowVectorBuilders

import java.nio.file.Paths
import java.time.Instant
import com.nec.arrow.functions.Sum.runOn
import com.nec.arrow.functions.Sum.sumJVM
import com.nec.aurora.Aurora
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.VeArrowNativeInterfaceNumeric
import com.nec.arrow.functions.Sum
import org.scalatest.freespec.AnyFreeSpec

final class SumVeSpec extends AnyFreeSpec {

  "We can get a sum back" in {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val libPath = VeKernelCompiler("sum", veBuildPath).compile_c(
      List(TransferDefinitions.TransferDefinitionsSourceCode, Sum.SumSourceCode)
        .mkString("\n\n")
    )
    val proc = Aurora.veo_proc_create(0)
    val (sum, expectedSum) =
      try {
        val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(proc)
        try {
          val data: Seq[Seq[Double]] = Seq(Seq(1, 2, 3, 4), Seq(10, 30, 50, 80))
          val lib: Long = Aurora.veo_load_library(proc, libPath.toString)
          ArrowVectorBuilders.withArrowFloat8Vector(data) { vcv =>
            (runOn(new VeArrowNativeInterfaceNumeric(proc, lib))(vcv, 4).sorted, sumJVM(vcv, 4).sorted)
          }
        } finally Aurora.veo_context_close(ctx)
      } finally Aurora.veo_proc_destroy(proc)

    assert(sum.nonEmpty)
    assert(sum == expectedSum)
  }
}
