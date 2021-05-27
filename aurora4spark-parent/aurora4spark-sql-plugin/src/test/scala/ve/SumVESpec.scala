package ve

import java.nio.file.Paths
import java.time.Instant

import cmake.SumCSpec.withArrowFloat8Vector
import com.nec.Sum.{runOn, sumJVM}
import com.nec.aurora.Aurora
import com.nec.native.{TransferDefinitions, VeArrowNativeInterfaceNumeric}
import com.nec.{Sum, VeCompiler}
import org.scalatest.freespec.AnyFreeSpec

final class SumVESpec extends AnyFreeSpec {

  "We can get a sum back" in {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val libPath = VeCompiler("sum", veBuildPath).compile_c(
      List(TransferDefinitions.TransferDefinitionsSourceCode, Sum.SumSourceCode)
        .mkString("\n\n")
    )
    val proc = Aurora.veo_proc_create(0)
    val (wordCount, expectedWordCount) =
      try {
        val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(proc)
        try {
          val data: Seq[Seq[Double]] = Seq(
            Seq(1, 2, 3, 4),
            Seq(10, 30, 50, 80)
          )
          val lib: Long = Aurora.veo_load_library(proc, libPath.toString)
          withArrowFloat8Vector(data) { vcv =>
            (
              runOn(new VeArrowNativeInterfaceNumeric(proc, ctx, lib))(vcv, 4),
              sumJVM(vcv,4 )
            )
          }
        } finally Aurora.veo_context_close(ctx)
      } finally Aurora.veo_proc_destroy(proc)

    assert(wordCount.nonEmpty)
    assert(wordCount == expectedWordCount)
  }
}
