package com.nec.jmh
import cats.effect.unsafe.implicits.global
import com.nec.jmh.AnalyzeSamplesSpec.TestSample
import org.apache.commons.io.IOUtils
import org.openjdk.jmh.profile.nec.StackSamplingProfiler.ThreadsSamples
import org.scalatest.freespec.AnyFreeSpec

import java.nio.charset.Charset

object AnalyzeSamplesSpec {
  import io.circe.generic.auto._
  val TestSample: ThreadsSamples = io.circe.parser
    .parse(IOUtils.resourceToString("/com/nec/jmh/thread-samples.json", Charset.defaultCharset()))
    .flatMap(_.as[ThreadsSamples])
    .fold(throw _, identity)
}

final class AnalyzeSamplesSpec extends AnyFreeSpec {
  "'SqlBaseParser' is reported" in {
    val str = AnalyzeSamples.apply(TestSample).unsafeRunSync()
    assert(str.contains("SqlBaseParser"))
  }
}
