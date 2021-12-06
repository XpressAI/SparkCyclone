package org.apache.spark.sql.vectorized

import org.apache.spark.sql.vectorized.DualMode.RichIterator
import org.scalatest.freespec.AnyFreeSpec

final class DualModeTest extends AnyFreeSpec {
  "empty iterator gives no items" in {
    assert(Iterator.empty.distinct.isEmpty)
  }
  "one item gives one item" in {
    assert(Iterator(1).distinct.toList == List(1))
  }
  "two same items give one item" in {
    assert(Iterator(1, 1).distinct.toList == List(1))
  }
  "two same items plus another give two" in {
    assert(Iterator(1, 1, 2).distinct.toList == List(1, 2))
  }
  "two distinct items give two items" in {
    assert(Iterator(1, 2).distinct.toList == List(1, 2))
  }
  "three same items plus a different give 2" in {
    assert(Iterator(1, 1, 1, 2).distinct.toList == List(1, 2))
  }
  "three same items plus a different and one more give 3" in {
    assert(Iterator(1, 1, 1, 2, 1).distinct.toList == List(1, 2, 1))
  }
}
