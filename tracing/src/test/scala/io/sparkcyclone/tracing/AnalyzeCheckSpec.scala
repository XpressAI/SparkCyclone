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
import io.sparkcyclone.tracing.SpanProcessor.{analyzeLines, RichL}
import io.sparkcyclone.tracing.TracingRecord.cleanPosition
import org.scalatest.freespec.AnyFreeSpec

import java.time.Duration

object AnalyzeCheckSpec {}
final class AnalyzeCheckSpec extends AnyFreeSpec {
  "Median of 4 durations is average of middle 2" in {
    assert(
      List(
        Duration.ofMinutes(1),
        Duration.ofMinutes(3),
        Duration.ofMinutes(9),
        Duration.ofMinutes(90)
      ).median
        == Duration.ofMinutes(6)
    )

  }
  "Median of 3 durations is the middle one" in {
    assert(
      List(Duration.ofMinutes(1), Duration.ofMinutes(3), Duration.ofMinutes(9)).median
        == Duration.ofMinutes(3)
    )
  }

  val sampleLines = List(
    "2021-10-18T10:19:20.003Z $ local-test|local-1634552356754|2021-10-18T10:19:20.003Z $$ S:prepare evaluator",
    "2021-10-18T10:19:28.874Z $ local-test|local-1634552356754|2021-10-18T10:19:20.003Z $$ E:prepare evaluator",
    "2021-10-18T10:19:29.861Z $ local-test|local-1634552356754|2021-10-18T10:19:20.003Z|driver|ad55 $$ S:evaluate a partition",
    "2021-10-18T10:19:30.123Z $ local-test|local-1634552356754|2021-10-18T10:19:20.003Z|driver|ad55 $$ E:evaluate a partition",
    "2021-10-18T10:19:30.134Z $ local-test|local-1634552356754|2021-10-18T10:19:20.003Z|driver|f952 $$ S:evaluate a partition",
    "2021-10-18T10:19:30.140Z $ local-test|local-1634552356754|2021-10-18T10:19:20.003Z|driver|f952 $$ E:evaluate a partition",
    "2021-10-17T14:30:31.272693000Z $ local-test|local-1634481005078|2021-10-17T14:30:05.198Z|driver|483f $$ S:Execution of Final:L556",
    "2021-10-17T14:30:31.275208000Z $ local-test|local-1634481005078|2021-10-17T14:30:05.198Z|driver|483f $$ E:Execution of Final:L686"
  )

  "We can aggregate by context" in {
    val expectedOutput = List(
      "[PT8.871S] prepare evaluator",
      "--",
      "[Count: 2, Median: PT0.134S,    Max: PT0.262S   ] evaluate a partition",
      "[Count: 1, Median: PT0.002515S, Max: PT0.002515S] Execution of Final",
      "--",
      "[Count: 2, Median: PT0.134S,    Max: PT0.262S   ] driver | evaluate a partition",
      "[Count: 1, Median: PT0.002515S, Max: PT0.002515S] driver | Execution of Final"
    )

    val result = analyzeLines(sampleLines)

    expect(result == expectedOutput)
  }
  "Line is removed from position" in {
    assert(cleanPosition("E:Execution of Final:L686") == "Execution of Final")
  }
}
