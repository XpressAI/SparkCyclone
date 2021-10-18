package com.nec.tracing

import com.eed3si9n.expecty.Expecty.expect
import com.nec.tracing.TracingListenerApp.safeAppId
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
      "2021-10-16T21:05:45.671390000Z $ Query 4|local-1634418262014|2021-10-16T21:04:27.833Z|9b97 $$ com.nec.spark.agile.StringProducer.FilteringProducer.complete#143/#452"
    val Some(tracingRecord) = TracingRecord
      .parse(inputString)
    import tracingRecord._

    expect(
      instant == Instant.parse("2021-10-16T21:05:45.671390000Z"),
      appName == "Query 4",
      appId == "local-1634418262014",
      executionId == Instant.parse("2021-10-16T21:04:27.833Z"),
      partId.contains("9b97"),
      position == "com.nec.spark.agile.StringProducer.FilteringProducer.complete#143/#452"
    )
  }
}
