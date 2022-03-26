package com.nec.util

import java.util.BitSet
import org.bytedeco.javacpp.BytePointer

object FixedBitSet {
  def from(buffer: BytePointer): FixedBitSet = {
    // Initialize the FixedBitSet
    val size = buffer.capacity().toInt * 8
    val bitset = FixedBitSet(size)

    // Copy the BytePointer buffer to the underlying bitset
    bitset.underlying = BitSet.valueOf(buffer.asBuffer)
    bitset
  }
}

case class FixedBitSet(size: Int) {
  // Set to var so we can efficiently assign from BytePointer
  private var underlying = new BitSet(size)

  def set(position: Int, value: Boolean): FixedBitSet = {
    underlying.set(position, value)
    this
  }

  def get(position: Int): Boolean = {
    underlying.get(position)
  }

  def toByteArray: Array[Byte] = {
    // The Array[Byte] is zero-initialized
    val bytes = new Array[Byte]((size / 8.0).ceil.toInt)

    /*
      BitSet saves storage space by only writing bytes out if there is a bit set
      and so the Array[Byte] can be of length 0 to (size / 8.0).ceil
    */
    val written = underlying.toByteArray

    // Copy the currently-written bytes in place
    for (i <- 0 until written.size) {
      bytes(i) = written(i)
    }

    bytes
  }

  def toSeq: Seq[Int] = {
    0.until(size).map(i => if (get(i)) 1 else 0).toSeq
  }

  def toBytePointer: BytePointer = {
    // Fetch the byte array representation of the bitset
    val bytes = toByteArray

    // Copy byte array over to BytePointer
    val buffer = new BytePointer(bytes.size.toLong)
    buffer.put(bytes, 0, bytes.size)
  }
}
