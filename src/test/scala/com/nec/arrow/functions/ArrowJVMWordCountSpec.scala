package com.nec.arrow.functions

import com.nec.arrow.ArrowVectorBuilders.withArrowStringVector
import org.scalatest.freespec.AnyFreeSpec

final class ArrowJVMWordCountSpec extends AnyFreeSpec {
  "JVM word count works" in {
    withArrowStringVector(Seq("hello", "test", "hello")) { vcv =>
      assert(WordCount.wordCountJVM(vcv) == Map("hello" -> 2, "test" -> 1))
    }
  }
}
