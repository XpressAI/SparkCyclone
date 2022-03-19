package com.nec.arrow.colvector

import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.nec.arrow.colvector.ArrayTOps._
import scala.reflect.ClassTag
import scala.util.Random
import java.util.UUID
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

class ArrayTOpsUnitSpec extends AnyWordSpec {
  "ArrayTOps" should {
    def runConversionTest[T <: AnyVal : ClassTag](input: Array[T]): Unit = {
      val name = s"${UUID.randomUUID}"
      val source = VeColVectorSource(s"${UUID.randomUUID}")
      val colvec = input.toBytePointerColVector(name)(source)

      // Check data
      colvec.underlying.veType.scalaType should be (implicitly[ClassTag[T]].runtimeClass)
      colvec.underlying.name should be (name)
      colvec.underlying.source should be(source)
      colvec.underlying.buffers.size should be (2)

      // Check validity buffer
      val validityBuffer = colvec.underlying.buffers(1).get
      validityBuffer.capacity() should be ((input.size / 8.0).ceil.toLong)
      for (i <- 0 until validityBuffer.capacity().toInt) {
        validityBuffer.get(i) should be (-1.toByte)
      }

      // Check conversion
      colvec.toArray[T] should be (input)
    }

    "correctly convert Array[Int] to BytePointerColVector and back" in {
      runConversionTest(0.to(Random.nextInt(100)).map(_ => Random.nextInt(10000)).toArray)
    }

    "correctly convert Array[Short] to BytePointerColVector and back" in {
      runConversionTest(0.to(Random.nextInt(100)).map(_ => Random.nextInt(10000).toShort).toArray)
    }

    "correctly convert Array[Long] to BytePointerColVector and back" in {
      runConversionTest(0.to(Random.nextInt(100)).map(_ => Random.nextLong).toArray)
    }

    "correctly convert Array[Float] to BytePointerColVector and back" in {
      runConversionTest(0.to(Random.nextInt(100)).map(_ => Random.nextFloat * 1000).toArray)
    }

    "correctly convert Array[Double] to BytePointerColVector and back" in {
      runConversionTest(0.to(Random.nextInt(100)).map(_ => Random.nextDouble * 1000).toArray)
    }

    "correctly convert Array[String] to BytePointerColVector and back" in {
      val input = 0.to(Random.nextInt(100)).map(_ => Random.nextString(Random.nextInt(30))).toArray
      val name = s"${UUID.randomUUID}"
      val source = VeColVectorSource(s"${UUID.randomUUID}")
      val colvec = input.toBytePointerColVector(name)(source)

      // Check data
      colvec.underlying.veType.scalaType should be (classOf[String])
      colvec.underlying.name should be (name)
      colvec.underlying.source should be(source)
      colvec.underlying.buffers.size should be (4)

      // Check validity buffer
      val validityBuffer = colvec.underlying.buffers(3).get
      validityBuffer.capacity() should be ((input.size / 8.0).ceil.toLong)
      for (i <- 0 until validityBuffer.capacity().toInt) {
        validityBuffer.get(i) should be (-1.toByte)
      }

      colvec.toArray[String] should be (input)
    }
  }
}
