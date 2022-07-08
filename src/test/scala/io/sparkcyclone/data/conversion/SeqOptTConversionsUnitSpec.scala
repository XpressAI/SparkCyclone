package io.sparkcyclone.data.conversion

import io.sparkcyclone.data.{InputSamples, VeColVectorSource}
import io.sparkcyclone.data.conversion.SeqOptTConversions._
import io.sparkcyclone.spark.agile.core.VeScalarType
import io.sparkcyclone.util.FixedBitSet
import scala.reflect.ClassTag
import java.util.UUID
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class SeqOptTConversionsUnitSpec extends AnyWordSpec {
  def runConversionTest[T <: AnyVal : ClassTag](input: Seq[Option[T]]): Unit = {
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
    0.until(input.size).foreach { i => bitset.get(i) should be (input(i).nonEmpty) }
    // Padding should be all zero's
    input.size.until((input.size / 64.0).ceil.toInt).foreach { i => bitset.get(i) should be (false) }

    // Check conversion
    colvec.toSeqOpt[T] should be (input)
  }

  "SeqOptTConversions" should {
    "correctly convert Seq[Option[Int]] to BytePointerColVector and back" in {
      runConversionTest(InputSamples.seqOpt[Int])
    }

    "correctly convert Seq[Option[Short]] to BytePointerColVector and back" in {
      runConversionTest(InputSamples.seqOpt[Short])
    }

    "correctly convert Seq[Option[Long]] to BytePointerColVector and back" in {
      runConversionTest(InputSamples.seqOpt[Long])
    }

    "correctly convert Seq[Option[Float]] to BytePointerColVector and back" in {
      runConversionTest(InputSamples.seqOpt[Float])
    }

    "correctly convert Seq[Option[Double]] to BytePointerColVector and back" in {
      runConversionTest(InputSamples.seqOpt[Double])
    }

    "correctly convert Seq[Option[String] to BytePointerColVector and back" in {
      val input = InputSamples.seqOpt[String]

      val name = s"${UUID.randomUUID}"
      val source = VeColVectorSource(s"${UUID.randomUUID}")
      val colvec = input.toBytePointerColVector(name)(source)

      // Check fields
      colvec.veType.scalaType should be (classOf[String])
      colvec.name should be (name)
      colvec.source should be (source)
      colvec.numItems should be (input.size)
      colvec.buffers.size should be (4)

      // Data, starts, and lens buffer capacities should be correctly set
      val dsize = input.foldLeft(0) { case (accum, x) =>
        accum + x.map(_.getBytes("UTF-32LE").size).getOrElse(0)
      }
      colvec.buffers(0).capacity() should be (dsize)
      colvec.buffers(1).capacity() should be (input.size.toLong * 4)
      colvec.buffers(2).capacity() should be (input.size.toLong * 4)

      // Validity buffer should be correctly set
      val bitset = FixedBitSet.from(colvec.buffers(3))
      0.until(input.size).foreach { i => bitset.get(i) should be (input(i).nonEmpty) }
      input.size.until((input.size / 64.0).ceil.toInt).foreach { i => bitset.get(i) should be (false) }

      // Check conversion - null String values should be preserved as well
      colvec.toSeqOpt[String] should be (input)
      colvec.toSeqOpt[UTF8String].map(_.map(_.toString)) should be (input)
    }
  }
}
