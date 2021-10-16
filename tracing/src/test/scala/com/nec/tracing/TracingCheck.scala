package com.nec.tracing

import fs2.Chunk
import org.scalatest.freespec.AnyFreeSpec

final class TracingCheck extends AnyFreeSpec {
  val samplechunk = Chunk[Byte](50, 48, 50, 49, 45, 49, 48, 45, 49, 54, 84, 49, 54, 58, 50, 51, 58,
    52, 56, 46, 56, 54, 57, 49, 54, 53, 48, 48, 48, 90, 32, 103, -12, 32, 99, 111, 109, 46, 110,
    101, 99, 46, 115, 112, 97, 114, 107, 46, 97, 103, 105, 108, 101, 46, 83, 116, 114, 105, 110,
    103, 80, 114, 111, 100, 117, 99, 101, 114, 46, 70, 105, 108, 116, 101, 114, 105, 110, 103, 80,
    114, 111, 100, 117, 99, 101, 114, 46, 99, 111, 109, 112, 108, 101, 116, 101, 35, 49, 52, 51, 32)
  "it works" in {
    println(new String(samplechunk.toArray))
  }
}
