package com.nec.colvector

import com.nec.colvector.ArrowVectorConversions._
import com.nec.spark.agile.core.VeScalarType
import scala.util.Random
import java.util.UUID
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class ArrowVectorConversionsUnitSpec extends AnyWordSpec {
  val allocator = new RootAllocator(Int.MaxValue)

  def runConversionTest[T <: ValueVector, U <: ValueVector](input: T, expectedO: Option[U] = None): Unit = {
    implicit val source = VeColVectorSource(s"${UUID.randomUUID}")
    val colvec = input.toBytePointerColVector

    // Check fields
    colvec.name should be (input.getName)
    colvec.source should be (source)
    colvec.numItems should be (input.getValueCount)

    // Check data buffer capacity
    if (input.isInstanceOf[BaseFixedWidthVector]) {
      val capacity = input.getValueCount * colvec.veType.asInstanceOf[VeScalarType].cSize
      colvec.buffers(0).capacity() should be (capacity)
    }

    // Check data, starts, and lens buffer capacities
    if (input.isInstanceOf[VarCharVector]) {
      val input0 = input.asInstanceOf[VarCharVector]
      val capacity = 0.until(input.getValueCount).foldLeft(0) { case (accum, i) =>
        val size = if (input0.isNull(i)) 0 else input0.getObject(i).toString.getBytes("UTF-32LE").size
        accum + size
      }

      colvec.buffers(0).capacity() should be (capacity)
      colvec.buffers(1).capacity() should be (input.getValueCount * 4)
      colvec.buffers(2).capacity() should be (input.getValueCount * 4)
    }

    // Convert back
    val output = colvec.toArrowVector(allocator)

    // Check conversion
    output === expectedO.getOrElse(input) should be (true)
  }

  "ArrowVectorConversions" should {
    "correctly perform equality checks between two ValueVectors" in {
      val raw = 0.until(Random.nextInt(100) + 100).map(_ => Random.nextInt(10000))
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
      val raw = 0.until(Random.nextInt(100)).map(_ => Random.nextInt(10000))
      val input = new IntVector(s"${UUID.randomUUID}", allocator)
      input.setValueCount(raw.length)
      raw.zipWithIndex.foreach { case (v, i) =>
        input.set(i, v)
      }
      // Set some validity bit to false
      if (raw.size > 0) input.setNull(Random.nextInt(raw.length))

      runConversionTest(input)
    }

    "correctly convert ShortVector to BytePointerColVector and back" in {
      val raw = 0.until(Random.nextInt(100)).map(_ => Random.nextInt(10000).toShort)
      val input = new SmallIntVector(s"${UUID.randomUUID}", allocator)
      input.setValueCount(raw.length)
      raw.zipWithIndex.foreach { case (v, i) =>
        input.set(i, v)
      }
      if (raw.size > 0) input.setNull(Random.nextInt(raw.length))

      runConversionTest(input)
    }

    "correctly convert BigIntVector to BytePointerColVector and back" in {
      val raw = 0.until(Random.nextInt(100)).map(_ => Random.nextLong)
      val input = new BigIntVector(s"${UUID.randomUUID}", allocator)
      input.setValueCount(raw.length)
      raw.zipWithIndex.foreach { case (v, i) =>
        input.set(i, v)
      }
      if (raw.size > 0) input.setNull(Random.nextInt(raw.length))

      runConversionTest(input)
    }

    "correctly convert Float4Vector to BytePointerColVector and back" in {
      val raw = 0.until(Random.nextInt(100)).map(_ => Random.nextFloat * 1000)
      val input = new Float4Vector(s"${UUID.randomUUID}", allocator)
      input.setValueCount(raw.length)
      raw.zipWithIndex.foreach { case (v, i) =>
        input.set(i, v)
      }
      if (raw.size > 0) input.setNull(Random.nextInt(raw.length))

      runConversionTest(input)
    }

    "correctly convert Float8Vector to BytePointerColVector and back" in {
      val raw = 0.until(Random.nextInt(100)).map(_ => Random.nextDouble * 1000)
      val input = new Float8Vector(s"${UUID.randomUUID}", allocator)
      input.setValueCount(raw.length)
      raw.zipWithIndex.foreach { case (v, i) =>
        input.set(i, v)
      }
      if (raw.size > 0) input.setNull(Random.nextInt(raw.length))

      runConversionTest(input)
    }

    "correctly convert DateDayVector to BytePointerColVector and back" in {
      val raw = 0.until(Random.nextInt(100)).map(_ => Random.nextInt(10000))

      val input = new DateDayVector(s"${UUID.randomUUID}", allocator)
      input.setValueCount(raw.length)
      raw.zipWithIndex.foreach { case (v, i) =>
        input.set(i, v)
      }

      val expected = new IntVector(input.getName, allocator)
      expected.setValueCount(raw.length)
      raw.zipWithIndex.foreach { case (v, i) =>
        expected.set(i, v)
      }

      if (raw.size > 0) {
        val i = Random.nextInt(raw.length)
        input.setNull(i)
        expected.setNull(i)
      }

      runConversionTest(input, Some(expected))
    }

    "correctly convert VarCharVector to BytePointerColVector and back" in {
      val raw = 0.until(Random.nextInt(100)).map(_ => Random.nextString(Random.nextInt(30)))
      val input = new VarCharVector(s"${UUID.randomUUID}", allocator)
      input.allocateNew()
      input.setValueCount(raw.length)
      raw.zipWithIndex.foreach { case (v, i) =>
        input.set(i, v.getBytes)
      }
      if (raw.size > 0) input.setNull(Random.nextInt(raw.length))

      runConversionTest(input)
    }
  }
}
