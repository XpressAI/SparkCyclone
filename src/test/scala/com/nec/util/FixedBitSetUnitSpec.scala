package com.nec.util

import scala.util.Random
import java.util.BitSet
import org.bytedeco.javacpp.BytePointer
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

class FixedBitSetUnitSpec extends AnyWordSpec {
  "FixedBitSet" should {
    "correctly set and get bits" in {
      val size = Random.nextInt(10) + 10
      val indices = Random.shuffle(0.until(size).toList).take(Random.nextInt(size))

      val bitset = FixedBitSet(size)
      val array = 0.until(size).map(_ => 0).toArray
      bitset.toSeq should be (array)

      // Setting a bit works
      indices.foreach { i =>
        bitset.set(i, true)
        array(i) = 1
      }
      bitset.toSeq should be (array)

      // Unsetting a bit works
      indices.foreach { i =>
        bitset.set(i, false)
        array(i) = 0
      }
      bitset.toSeq should be (array)
    }

    "correctly serialize into Array[Byte]" in {
      val size = Random.nextInt(10) + 10
      val indices = Random.shuffle(0.until(size).toList).take(Random.nextInt(size))

      val bitset1 = FixedBitSet(size)
      indices.foreach { i =>
        bitset1.set(i, true)
      }

      val bytes = bitset1.toByteArray
      bytes.size should be ((size / 64.0).ceil.toInt * 8)

      val bitset2 = BitSet.valueOf(bytes)
      for (i <- 0 until size) {
        bitset1.get(i) should be (bitset2.get(i))
      }
    }

    "correctly serialize into BytePointer" in {
      // Keep in byte-aligned lengths for easier test check
      val size = (Random.nextInt(10) + 1) * 8
      val indices = Random.shuffle(0.until(size).toList).take(Random.nextInt(size))

      val bitset1 = FixedBitSet(size)
      indices.foreach { i =>
        bitset1.set(i, true)
      }

      val bitset2 = FixedBitSet.from(bitset1.toBytePointer)
      bitset1.toSeq should be (bitset2.toSeq.take(bitset1.size))
    }

    // "correctly create an Array[Byte] of 1-bits" in {
    //   val size = (Random.nextInt(10) + 1)

    //   val bytes = FixedBitSet.from(FixedBitSet.ones(size)).toByteArray
    //   bytes.size should be ((size / 64.0).ceil * 8)
    //   bytes.foreach(b => b should be (-1.toByte))
    // }
  }
}
