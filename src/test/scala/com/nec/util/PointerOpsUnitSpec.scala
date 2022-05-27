package com.nec.util

import com.nec.util.PointerOps._
import scala.util.Random
import org.bytedeco.javacpp._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

class PointerOpsUnitSpec extends AnyWordSpec {
  "ExtendedPointer" should {
    "correctly determine the size of the buffer in bytes" in {
      val buffer1 = new BytePointer(Random.nextInt(1000).toLong)
      buffer1.nbytes should be (buffer1.limit() * 1)

      val buffer2 = new ShortPointer(Random.nextInt(1000).toLong)
      buffer2.nbytes should be (buffer2.limit() * 2)

      val buffer3 = new IntPointer(Random.nextInt(1000).toLong)
      buffer3.nbytes should be (buffer3.limit() * 4)

      val buffer4 = new LongPointer(Random.nextInt(1000).toLong)
      buffer4.nbytes should be (buffer4.limit() * 8)

      val buffer5 = new DoublePointer(Random.nextInt(1000).toLong)
      buffer5.nbytes should be (buffer5.limit() * 8)
    }
  }

  "ExtendedBytePointer" should {
    "correctly convert to Array[Byte]" in {
      val size = Random.nextInt(100) + 1
      val buffer = new BytePointer(size.toLong)
      0.until(size).foreach { i => buffer.put(i.toLong, i.toByte) }

      buffer.toArray.toSeq should be (0.until(size).toSeq)
    }

    "correctly create ByteBuffer slices" in {
      val buffer = new BytePointer(100L)
      0.until(100).foreach { i => buffer.put(i.toLong, i.toByte) }

      val offset = Random.nextInt(30)
      val size = Random.nextInt(30)
      buffer.slice(offset, size).toArray.toSeq should be (offset.until(offset + size).toSeq)
    }

    "correctly convert to hex array" in {
      val size = Random.nextInt(100) + 1
      val buffer = new BytePointer(size.toLong)
      0.until(size).foreach { i => buffer.put(i.toLong, i.toByte) }

      buffer.toHex should be (0.until(size).toSeq.map { i => String.format("%02x", Byte.box(i.toByte)) })
    }

    "correctly be materialized into another Pointer class" in {
      val size = Random.nextInt(100) + 1
      val buffer = new BytePointer(size * 4)
      val ibuf = new IntPointer(buffer)

      val array = 0.until(size).toArray.map(_ => Random.nextInt(10000))
      ibuf.put(array, 0, array.size)

      buffer.as[IntPointer].toArray should be (array)
    }
  }
}
