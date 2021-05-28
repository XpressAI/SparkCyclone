package ve

import cmake.CountStringsCSpec.withArrowStringVector
import com.nec.aurora.Aurora
import org.scalatest.freespec.AnyFreeSpec

import java.nio.file.Paths
import java.time.Instant
import ve.CountStringsVESpec.Sample
import com.nec.WordCount.runOn
import com.nec.WordCount.wordCountJVM
import com.nec.VeCompiler
import com.nec.WordCount
import com.nec.native.TransferDefinitions
import com.nec.native.VeArrowNativeInterface
object CountStringsVESpec {
  val Sample = List[String]("hello", "dear", "world", "of", "hello", "of", "hello")
}

final class CountStringsVESpec extends AnyFreeSpec {

  "We can get a word count back" in {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val libPath = VeCompiler("wc", veBuildPath).compile_c(
      List(TransferDefinitions.TransferDefinitionsSourceCode, WordCount.WordCountSourceCode)
        .mkString("\n\n")
    )
    val proc = Aurora.veo_proc_create(0)
    val (wordCount, expectedWordCount) =
      try {
        val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(proc)
        try {
          val lib: Long = Aurora.veo_load_library(proc, libPath.toString)
          withArrowStringVector(Sample) { vcv =>
            (runOn(new VeArrowNativeInterface(proc, ctx, lib))(vcv), wordCountJVM(vcv))
          }
        } finally Aurora.veo_context_close(ctx)
      } finally Aurora.veo_proc_destroy(proc)

    assert(wordCount.nonEmpty)
    assert(wordCount == expectedWordCount)
  }
}
