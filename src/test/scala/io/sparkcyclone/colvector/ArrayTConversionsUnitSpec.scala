package io.sparkcyclone.colvector

import io.sparkcyclone.colvector.ArrayTConversions._
import io.sparkcyclone.colvector.SeqOptTConversions._
import io.sparkcyclone.spark.agile.core.VeScalarType
import io.sparkcyclone.util.FixedBitSet
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import java.util.UUID
import scala.reflect.ClassTag
import scala.util.Random

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

    // Data buffer size should be correctly set
    colvec.buffers(0).capacity() should be (input.size.toLong * colvec.veType.asInstanceOf[VeScalarType].cSize)

    // Check validity buffer
    val validityBuffer = colvec.buffers(1)
    validityBuffer.capacity() should be ((input.size / 64.0).ceil.toLong * 8)

    val bitset = FixedBitSet.from(validityBuffer)
    // Bitset should be correctly set
    input.indices.foreach { i => bitset.get(i) should be (true) }
    // Padding should be all zero's
    input.size.until((input.size / 64.0).ceil.toInt).foreach { i => bitset.get(i) should be (false) }

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
      val dsize = input.foldLeft(0) { case (accum, x) =>
        accum + (if (x == null) 0 else x.getBytes("UTF-32LE").size)
      }
      colvec.buffers(0).capacity() should be (dsize)
      colvec.buffers(1).capacity() should be (input.size.toLong * 4)
      colvec.buffers(2).capacity() should be (input.size.toLong * 4)

      // Validity buffer should be correctly set
      val bitset = FixedBitSet.from(colvec.buffers(3))
      0.until(input.size).foreach { i => bitset.get(i) should be (input(i) != null) }
      input.size.until((input.size / 64.0).ceil.toInt).foreach { i => bitset.get(i) should be (false) }

      // Check conversion - null String values should be preserved as well
      colvec.toArray[String] should be (input)
      colvec.toSeqOpt[String] should be (input.map(Option(_)))
    }
  }
}
