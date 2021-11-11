/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
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
