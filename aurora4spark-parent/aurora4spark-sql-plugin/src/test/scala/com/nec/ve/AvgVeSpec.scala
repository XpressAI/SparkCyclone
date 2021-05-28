package com.nec.ve

import com.nec.arrow.ArrowVectorBuilders.withArrowFloat8Vector
import java.nio.file.Paths
import java.time.Instant
import com.nec.arrow.functions.Avg.avgJVM
import com.nec.arrow.functions.Avg.runOn
import com.nec.aurora.Aurora
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.VeArrowNativeInterfaceNumeric
import com.nec.arrow.functions.Avg
import org.scalatest.freespec.AnyFreeSpec

final class AvgVeSpec extends AnyFreeSpec {

  "We can get a avg back" in {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val libPath = VeKernelCompiler("avg", veBuildPath).compile_c(
      List(TransferDefinitions.TransferDefinitionsSourceCode, Avg.AvgSourceCode)
        .mkString("\n\n")
    )
    val proc = Aurora.veo_proc_create(0)
    val (avg, expectedAvg) =
      try {
        val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(proc)
        try {
          val data: Seq[Seq[Double]] = Seq(Seq(1, 2, 3, 4), Seq(10, 30, 50, 80))
          val lib: Long = Aurora.veo_load_library(proc, libPath.toString)
          withArrowFloat8Vector(data) { vcv =>
            (runOn(new VeArrowNativeInterfaceNumeric(lib))(vcv, 4), avgJVM(vcv, 4))
          }
        } finally Aurora.veo_context_close(ctx)
      } finally Aurora.veo_proc_destroy(proc)

    assert(avg.nonEmpty)
    assert(avg == expectedAvg)
  }
}
