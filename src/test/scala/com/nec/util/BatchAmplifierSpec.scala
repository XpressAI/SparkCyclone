package com.nec.util

import org.scalatest.freespec.AnyFreeSpec

final class BatchAmplifierSpec extends AnyFreeSpec {
  def amplifyInt(l: List[Int]): List[Vector[Int]] =
    BatchAmplifier.amplify[Int](l.iterator)(5, i => i).toList
  "It passes through an empty set" in {
    assert(amplifyInt(List.empty).isEmpty)
  }
  "It aggregates 1 item only" in {
    assert(amplifyInt(List(1)) == List(List(1)))
  }
  "It aggregates just slightly over the limit of 5" in {
    assert(amplifyInt(List(1, 2, 3, 4, 6, 1, 3)) == List(List(1, 2, 3), List(4, 6), List(1, 3)))
  }
  "It aggregates just slightly over the limit of 5 (2)" in {
    assert(amplifyInt(List(1, 2, 3, 4, 6, 1)) == List(List(1, 2, 3), List(4, 6), List(1)))
  }
}
