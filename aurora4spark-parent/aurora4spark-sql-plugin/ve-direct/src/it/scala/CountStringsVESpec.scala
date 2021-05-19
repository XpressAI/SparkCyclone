import com.nec.aurora.Aurora
import org.scalatest.freespec.AnyFreeSpec

import java.nio.file.Paths
import java.time.Instant
import scala.sys.process._
import CountStringsVESpec._
import com.nec.{VeCompiler, WordCount}
import com.nec.WordCount.SomeStrings
object CountStringsVESpec {
  val Sample: SomeStrings = SomeStrings("hello", "dear", "world", "of", "hello", "of", "hello")
}

final class CountStringsVESpec extends AnyFreeSpec {
  "We can get a word count back" in {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val libPath = VeCompiler("wc", veBuildPath).compile_c(WordCount.SourceCode)
    val proc = Aurora.veo_proc_create(0)
    val expectedWordCount = Sample.expectedWordCount
    val wordCount =
      try {
        val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(proc)
        try {
          Sample.computeVE(proc, ctx, libPath)
        } finally Aurora.veo_context_close(ctx)
      } finally Aurora.veo_proc_destroy(proc)
    info(s"Got: $wordCount")
    assert(wordCount == expectedWordCount)
  }
}
