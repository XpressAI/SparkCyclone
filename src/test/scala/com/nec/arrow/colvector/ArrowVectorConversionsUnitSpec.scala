package com.nec.arrow.colvector

import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.nec.arrow.colvector.ArrowVectorConversions._
import scala.util.Random
import java.util.UUID
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

class ArrowVectorConversionsUnitSpec extends AnyWordSpec {
  val allocator = new RootAllocator(Int.MaxValue)

  def runConversionTest[T <: ValueVector](input: T): Unit = {
    val source = VeColVectorSource(s"${UUID.randomUUID}")
    val colvec = input.toBytePointerColVector(source)

    // Check data
    colvec.underlying.name should be (input.getName)
    colvec.underlying.source should be (source)

    // Convert back
    val output = colvec.toArrowVector(allocator)

    // Check conversion
    input === output should be (true)
  }

  "ArrowVectorConversions" should {
    "correctly perform equality checks between two ValueVectors" in {
      val raw = 0.to(Random.nextInt(100)).map(_ => Random.nextInt(10000))
      val vec1 = new IntVector(s"${UUID.randomUUID}", allocator)
      vec1.setValueCount(raw.length)
      raw.zipWithIndex.foreach { case (v, i) =>
        vec1.set(i, v)
      }

      // ValueVectors need to have the same name for the equality check to pass
      val vec2 = new IntVector(vec1.getName, allocator)
      vec2.setValueCount(raw.length)
      raw.zipWithIndex.foreach { case (v, i) =>
        vec2.set(i, v)
      }

      // ValueVectors should be equal
      vec1 === vec2 should be (true)

      val index = Random.nextInt(raw.size)
      val tmp = vec2.get(index)

      // Set row to null
      vec2.setNull(index)
      vec1 === vec2 should be (false)

      // Set row to another value
      vec2.set(index, Random.nextInt(10000) + 1000)
      vec1 === vec2 should be (false)

      // Set row back to original value
      vec2.set(index, tmp)
      vec1 === vec2 should be (true)
    }

    "correctly convert IntVector to BytePointerColVector and back" in {
      val raw = 0.to(Random.nextInt(100)).map(_ => Random.nextInt(10000))
      val input = new IntVector(s"${UUID.randomUUID}", allocator)
      input.setValueCount(raw.length)
      raw.zipWithIndex.foreach { case (v, i) =>
        input.set(i, v)
      }
      // Set some validity bit to false
      input.setNull(Random.nextInt(raw.length))

      runConversionTest(input)
    }

    "correctly convert ShortVector to BytePointerColVector and back" in {
      val raw = 0.to(Random.nextInt(100)).map(_ => Random.nextInt(10000).toShort)
      val input = new SmallIntVector(s"${UUID.randomUUID}", allocator)
      input.setValueCount(raw.length)
      raw.zipWithIndex.foreach { case (v, i) =>
        input.set(i, v)
      }
      input.setNull(Random.nextInt(raw.length))

      runConversionTest(input)
    }

    "correctly convert LongVector to BytePointerColVector and back" in {
      val raw = 0.to(Random.nextInt(100)).map(_ => Random.nextLong)
      val input = new BigIntVector(s"${UUID.randomUUID}", allocator)
      input.setValueCount(raw.length)
      raw.zipWithIndex.foreach { case (v, i) =>
        input.set(i, v)
      }
      input.setNull(Random.nextInt(raw.length))

      runConversionTest(input)
    }

    "correctly convert Float4Vector to BytePointerColVector and back" in {
      val raw = 0.to(Random.nextInt(100)).map(_ => Random.nextFloat * 1000)
      val input = new Float4Vector(s"${UUID.randomUUID}", allocator)
      input.setValueCount(raw.length)
      raw.zipWithIndex.foreach { case (v, i) =>
        input.set(i, v)
      }
      input.setNull(Random.nextInt(raw.length))

      runConversionTest(input)
    }

    "correctly convert Float8Vector to BytePointerColVector and back" in {
      val raw = 0.to(Random.nextInt(100)).map(_ => Random.nextDouble * 1000)
      val input = new Float8Vector(s"${UUID.randomUUID}", allocator)
      input.setValueCount(raw.length)
      raw.zipWithIndex.foreach { case (v, i) =>
        input.set(i, v)
      }
      input.setNull(Random.nextInt(raw.length))

      runConversionTest(input)
    }

    "correctly convert VarCharVector to BytePointerColVector and back" in {
      val raw = 0.to(Random.nextInt(100)).map(_ => Random.nextString(Random.nextInt(30)))
      val input = new VarCharVector(s"${UUID.randomUUID}", allocator)
      input.allocateNew()
      input.setValueCount(raw.length)
      raw.zipWithIndex.foreach { case (v, i) =>
        input.set(i, v.getBytes)
      }
      input.setNull(Random.nextInt(raw.length))

      runConversionTest(input)
    }
  }
}
