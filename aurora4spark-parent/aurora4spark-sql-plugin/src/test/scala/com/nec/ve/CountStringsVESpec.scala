package com.nec.ve

import com.nec.arrow.ArrowVectorBuilders
import com.nec.aurora.Aurora
import org.scalatest.freespec.AnyFreeSpec

import java.nio.file.Paths
import java.time.Instant
import com.nec.ve.CountStringsVESpec.Sample
import com.nec.arrow.functions.WordCount.runOn
import com.nec.arrow.functions.WordCount.wordCountJVM
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.VeArrowNativeInterface
import com.nec.arrow.functions.WordCount
object CountStringsVESpec {
  val Sample = List[String]("hello", "dear", "world", "of", "hello", "of", "hello")
}

final class CountStringsVESpec extends AnyFreeSpec {

  "We can get a word count back" in {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val libPath = VeKernelCompiler("wc", veBuildPath).compile_c(
      List(TransferDefinitions.TransferDefinitionsSourceCode, WordCount.WordCountSourceCode)
        .mkString("\n\n")
    )
    val proc = Aurora.veo_proc_create(0)
    val (wordCount, expectedWordCount) =
      try {
        val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(proc)
        try {
          val lib: Long = Aurora.veo_load_library(proc, libPath.toString)
          ArrowVectorBuilders.withArrowStringVector(Sample) { vcv =>
            (runOn(new VeArrowNativeInterface(lib))(vcv), wordCountJVM(vcv))
          }
        } finally Aurora.veo_context_close(ctx)
      } finally Aurora.veo_proc_destroy(proc)

    assert(wordCount.nonEmpty)
    assert(wordCount == expectedWordCount)
  }
}
