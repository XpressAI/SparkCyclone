package com.nec.tracing

import com.eed3si9n.expecty.Expecty.expect
import com.nec.tracing.TracingCheck.SampleChunk
import fs2.Chunk
import org.scalatest.freespec.AnyFreeSpec

import java.time.Instant

object TracingCheck {
  val SampleChunk: Chunk[Byte] = Chunk[Byte](50, 48, 50, 49, 45, 49, 48, 45, 49, 54, 84, 49, 57, 58,
    51, 55, 58, 53, 55, 46, 48, 48, 55, 50, 53, 52, 48, 48, 48, 90, 32, 36, 32, 91, 108, 97, 117,
    110, 99, 104, 73, 100, 45, 109, 97, 112, 112, 105, 110, 103, 73, 100, 93, 32, 36, 36, 32, 99,
    111, 109, 46, 110, 101, 99, 46, 99, 109, 97, 107, 101, 46, 84, 114, 97, 99, 101, 114, 84, 101,
    115, 116, 46, 97, 110, 105, 35, 50, 55, 47, 35, 50, 54, 54, 10)
}
final class TracingCheck extends AnyFreeSpec {
  "it works" in {
    val str = new String(SampleChunk.toArray)
    val Some(TracingRecord(instant, recordId, position)) = TracingRecord
      .parse(str)

    expect(
      instant == Instant.parse("2021-10-16T19:37:57.007254000Z"),
      recordId == "[launchId-mappingId]",
      position == "com.nec.cmake.TracerTest.ani#27/#266"
    )
  }
}
