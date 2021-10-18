package com.nec.tracing

import com.eed3si9n.expecty.Expecty.expect
import com.nec.tracing.SpanProcessor.analyzeLines
import com.nec.tracing.TracingRecord.cleanPosition
import org.scalatest.freespec.AnyFreeSpec

object AnalyzeCheckSpec {}
final class AnalyzeCheckSpec extends AnyFreeSpec {
  val sampleLines = List(
    "2021-10-18T10:19:20.003Z $ local-test|local-1634552356754|2021-10-18T10:19:20.003Z $$ S:prepare evaluator",
    "2021-10-18T10:19:28.874Z $ local-test|local-1634552356754|2021-10-18T10:19:20.003Z $$ E:prepare evaluator",
    "2021-10-18T10:19:29.861Z $ local-test|local-1634552356754|2021-10-18T10:19:20.003Z|ad55 $$ S:evaluate a partition",
    "2021-10-18T10:19:30.123Z $ local-test|local-1634552356754|2021-10-18T10:19:20.003Z|ad55 $$ E:evaluate a partition",
    "2021-10-18T10:19:30.134Z $ local-test|local-1634552356754|2021-10-18T10:19:20.003Z|f952 $$ S:evaluate a partition",
    "2021-10-18T10:19:30.140Z $ local-test|local-1634552356754|2021-10-18T10:19:20.003Z|f952 $$ E:evaluate a partition",
    "2021-10-17T14:30:31.272693000Z $ local-test|local-1634481005078|2021-10-17T14:30:05.198Z|483f $$ S:Execution of Final:L556",
    "2021-10-17T14:30:31.275208000Z $ local-test|local-1634481005078|2021-10-17T14:30:05.198Z|483f $$ E:Execution of Final:L686"
  )

  "We can aggregate by context" in {
    val expectedOutput = List(
      "[PT8.871S] prepare evaluator",
      "[Total: PT0.268S, Count: 2, Average: PT0.134S, Max: PT0.262S] evaluate a partition",
      "[Total: PT0.002515S, Count: 1, Average: PT0.002515S, Max: PT0.002515S] Execution of Final"
    )
    val result = analyzeLines(sampleLines)
    expect(result == expectedOutput)
  }
  "Line is removed from position" in {
    assert(cleanPosition("E:Execution of Final:L686") == "Execution of Final")
  }
}
