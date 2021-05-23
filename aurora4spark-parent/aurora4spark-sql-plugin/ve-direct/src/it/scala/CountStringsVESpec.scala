import CountStringsCSpec.withArrowStringVector
import com.nec.aurora.Aurora
import org.scalatest.freespec.AnyFreeSpec

import java.nio.file.Paths
import java.time.Instant
import scala.sys.process._
import CountStringsVESpec._
import com.nec.WordCount.{runOn, wordCountJVM}
import com.nec.native.VeArrowNativeInterface
import com.nec.{VeCompiler, WordCount}
object CountStringsVESpec {
  val Sample = List[String]("hello", "dear", "world", "of", "hello", "of", "hello")
}

final class CountStringsVESpec extends AnyFreeSpec {

  "We can get a word count back" in {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val libPath = VeCompiler("wc", veBuildPath).compile_c(WordCount.WordCountSourceCode)
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
