package com.nec.colvector

import com.nec.colvector.ArrayTConversions._
import com.nec.colvector.SeqOptTConversions._
import com.nec.spark.agile.core.VeScalarType
import scala.reflect.ClassTag
import scala.util.Random
import java.util.UUID
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class ArrayTConversionsUnitSpec extends AnyWordSpec {
  def runConversionTest[T <: AnyVal : ClassTag](input: Array[T]): Unit = {
    implicit val source = VeColVectorSource(s"${UUID.randomUUID}")
    val name = s"${UUID.randomUUID}"
    val colvec = input.toBytePointerColVector(name)

    // Check fields
    colvec.veType.scalaType should be (implicitly[ClassTag[T]].runtimeClass)
    colvec.name should be (name)
    colvec.source should be (source)
    colvec.numItems should be (input.size)
    colvec.buffers.size should be (2)

    // Data buffer capacity should be correctly set
    colvec.buffers(0).capacity() should be (input.size.toLong * colvec.veType.asInstanceOf[VeScalarType].cSize)

    // Check validity buffer
    val validityBuffer = colvec.buffers(1)
    validityBuffer.capacity() should be ((input.size / 64.0).ceil.toLong * 8)
    for (i <- 0 until validityBuffer.capacity().toInt) {
      validityBuffer.get(i) should be (-1.toByte)
    }

    // Check conversion
    colvec.toArray[T] should be (input)
  }

  "ArrayTConversions" should {
    "correctly convert Array[Int] to BytePointerColVector and back" in {
      runConversionTest(InputSamples.array[Int])
    }

    "correctly convert Array[Short] to BytePointerColVector and back" in {
      runConversionTest(InputSamples.array[Short])
    }

    "correctly convert Array[Long] to BytePointerColVector and back" in {
      runConversionTest(InputSamples.array[Long])
    }

    "correctly convert Array[Float] to BytePointerColVector and back" in {
      runConversionTest(InputSamples.array[Float])
    }

    "correctly convert Array[Double] to BytePointerColVector and back" in {
      runConversionTest(InputSamples.array[Double])
    }

    "correctly convert Array[String] to BytePointerColVector and back" in {
      val input = InputSamples.array[String]
      // Set one of the values to null
      if (input.size > 0) input(Random.nextInt(input.size)) = null

      val name = s"${UUID.randomUUID}"
      val source = VeColVectorSource(s"${UUID.randomUUID}")
      val colvec = input.toBytePointerColVector(name)(source)

      // Check fields
      colvec.veType.scalaType should be (classOf[String])
      colvec.name should be (name)
      colvec.source should be(source)
      colvec.buffers.size should be (4)

      // Data, starts, and lens buffer capacities should be correctly set
      val capacity = input.foldLeft(0) { case (accum, x) =>
        accum + (if (x == null) 0 else x.getBytes("UTF-32LE").size)
      }
      colvec.buffers(0).capacity() should be (capacity)
      colvec.buffers(1).capacity() should be (input.size.toLong * 4)
      colvec.buffers(2).capacity() should be (input.size.toLong * 4)

      // Check conversion - null String values should be preserved as well
      colvec.toArray[String] should be (input)
      colvec.toSeqOpt[String] should be (input.map(Option(_)))
    }
  }
}
