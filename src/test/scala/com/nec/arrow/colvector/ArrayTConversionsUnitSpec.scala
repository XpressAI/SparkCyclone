package com.nec.arrow.colvector

import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.nec.arrow.colvector.ArrayTConversions._
import com.nec.spark.agile.core.VeScalarType
import scala.reflect.ClassTag
import scala.util.Random
import java.util.UUID
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

class ArrayTConversionsUnitSpec extends AnyWordSpec {
  def runConversionTest[T <: AnyVal : ClassTag](input: Array[T]): Unit = {
    implicit val source = VeColVectorSource(s"${UUID.randomUUID}")
    val name = s"${UUID.randomUUID}"
    val colvec = input.toBytePointerColVector(name)

    // Check fields
    colvec.underlying.veType.scalaType should be (implicitly[ClassTag[T]].runtimeClass)
    colvec.underlying.name should be (name)
    colvec.underlying.source should be (source)
    colvec.underlying.numItems should be (input.size)
    colvec.underlying.buffers.size should be (2)

    // Data buffer capacity should be correctly set
    colvec.underlying.buffers(0).get.capacity() should be (input.size.toLong * colvec.underlying.veType.asInstanceOf[VeScalarType].cSize)

    // Check validity buffer
    val validityBuffer = colvec.underlying.buffers(1).get
    validityBuffer.capacity() should be ((input.size / 8.0).ceil.toLong)
    for (i <- 0 until validityBuffer.capacity().toInt) {
      validityBuffer.get(i) should be (-1.toByte)
    }

    // Check conversion
    colvec.toArray[T] should be (input)
  }

  "ArrayTConversions" should {
    "correctly convert Array[Int] to BytePointerColVector and back" in {
      runConversionTest(0.until(Random.nextInt(100)).map(_ => Random.nextInt(10000)).toArray)
    }

    "correctly convert Array[Short] to BytePointerColVector and back" in {
      runConversionTest(0.until(Random.nextInt(100)).map(_ => Random.nextInt(10000).toShort).toArray)
    }

    "correctly convert Array[Long] to BytePointerColVector and back" in {
      runConversionTest(0.until(Random.nextInt(100)).map(_ => Random.nextLong).toArray)
    }

    "correctly convert Array[Float] to BytePointerColVector and back" in {
      runConversionTest(0.until(Random.nextInt(100)).map(_ => Random.nextFloat * 1000).toArray)
    }

    "correctly convert Array[Double] to BytePointerColVector and back" in {
      runConversionTest(0.until(Random.nextInt(100)).map(_ => Random.nextDouble * 1000).toArray)
    }

    "correctly convert Array[String] to BytePointerColVector and back" in {
      val input = 0.until(Random.nextInt(100)).map(_ => Random.nextString(Random.nextInt(30))).toArray
      // Set one of the values to null
      if (input.size > 0) input(Random.nextInt(input.size)) = null

      val name = s"${UUID.randomUUID}"
      val source = VeColVectorSource(s"${UUID.randomUUID}")
      val colvec = input.toBytePointerColVector(name)(source)

      // Check fields
      colvec.underlying.veType.scalaType should be (classOf[String])
      colvec.underlying.name should be (name)
      colvec.underlying.source should be(source)
      colvec.underlying.buffers.size should be (4)

      // Data, starts, and lens buffer capacities should be correctly set
      val capacity = input.foldLeft(0) { case (accum, x) =>
        accum + (if (x == null) 0 else x.getBytes("UTF-32LE").size)
      }
      colvec.underlying.buffers(0).get.capacity() should be (capacity)
      colvec.underlying.buffers(1).get.capacity() should be (input.size.toLong * 4)
      colvec.underlying.buffers(2).get.capacity() should be (input.size.toLong * 4)

      // Check conversion - null String values should be preserved as well
      colvec.toArray[String] should be (input)
    }
  }
}
