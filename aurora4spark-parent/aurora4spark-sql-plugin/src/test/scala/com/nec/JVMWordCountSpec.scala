package com.nec

import cmake.CountStringsCSpec.withArrowStringVector
import org.scalatest.freespec.AnyFreeSpec

final class JVMWordCountSpec extends AnyFreeSpec {
  "JVM word count works" in {
    withArrowStringVector(Seq("hello", "test", "hello")) { vcv =>
      assert(WordCount.wordCountJVM(vcv) == Map("hello" -> 2, "test" -> 1))
    }
  }
}
