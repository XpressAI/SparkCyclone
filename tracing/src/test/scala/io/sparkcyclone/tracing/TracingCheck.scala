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
package io.sparkcyclone.tracing

import com.eed3si9n.expecty.Expecty.expect
import io.sparkcyclone.tracing.TracingListenerApp.safeAppId
import org.scalatest.freespec.AnyFreeSpec

import java.time.Instant

object TracingCheck {}
final class TracingCheck extends AnyFreeSpec {
  "ID is unsafe" in {
    assert(!safeAppId("/"))
  }
  "Slash ID is unsafe" in {
    assert(!safeAppId("///////////"))
  }

  "Simple ID is safe" in {
    assert(safeAppId("local-1634418262014"))
  }

  "it works" in {
    val inputString =
      "2021-10-16T21:05:45.671390000Z $ Query 4|local-1634418262014|2021-10-16T21:04:27.833Z|9b97 $$ io.sparkcyclone.spark.codegen.StringProducer.FilteringProducer.complete#143/#452"
    val Some(tracingRecord) = TracingRecord
      .parse(inputString)
    import tracingRecord._

    expect(
      instant == Instant.parse("2021-10-16T21:05:45.671390000Z"),
      appName == "Query 4",
      appId == "local-1634418262014",
      executionId == Instant.parse("2021-10-16T21:04:27.833Z"),
      partId.contains("9b97"),
      position == "io.sparkcyclone.spark.codegen.StringProducer.FilteringProducer.complete#143/#452"
    )
  }
}
