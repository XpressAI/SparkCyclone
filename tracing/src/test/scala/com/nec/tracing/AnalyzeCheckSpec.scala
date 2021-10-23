package com.nec.tracing

import com.eed3si9n.expecty.Expecty.expect
import com.nec.tracing.SpanProcessor.{analyzeLines, RichL}
import com.nec.tracing.TracingRecord.cleanPosition
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
    "2021-10-17T14:30:31.272693000Z $ local-test|local-1634481005078|2021-10-17T14:30:05.198Z|driver|83f $$ S:Execution of Final:L556",
    "2021-10-17T14:30:31.275208000Z $ local-test|local-1634481005078|2021-10-17T14:30:05.198Z|driver|483f $$ E:Execution of Final:L686"
  )

  "We can aggregate by context" in {
    val expectedOutput = List(
      "[PT8.871S] prepare evaluator",
      "--",
      "[Count: 2, Median: PT0.134S,    Max: PT0.262S   ] evaluate a partition",
      "[Count: 1, Median: PT0.002515S, Max: PT0.002515S] Execution of Final",
      "--",
      "[Count: 2, Median: PT0.134S,    Max: PT0.262S   ] driver | evaluate a partition"
    )

    val result = analyzeLines(sampleLines)
    expect(result == expectedOutput)
  }
  "Line is removed from position" in {
    assert(cleanPosition("E:Execution of Final:L686") == "Execution of Final")
  }
}
