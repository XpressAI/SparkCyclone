package com.nec.arrow.colvector

import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.nec.arrow.colvector.SeqOptTConversions._
import com.nec.spark.agile.core.VeScalarType
import com.nec.util.FixedBitSet
import scala.reflect.ClassTag
import scala.util.Random
import java.util.UUID
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

class SeqOptTConversionsUnitSpec extends AnyWordSpec {
  def runConversionTest[T <: AnyVal : ClassTag](input: Seq[Option[T]]): Unit = {
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
    val bitset = FixedBitSet.from(validityBuffer)
    0.until(input.size).foreach(i => bitset.get(i) should be (input(i).nonEmpty))

    // Check conversion
    colvec.toSeqOpt[T] should be (input)
  }

  "SeqOptTConversions" should {
    "correctly convert Seq[Option[Int]] to BytePointerColVector and back" in {
      runConversionTest(0.until(Random.nextInt(100)).map(_ => if (Math.random < 0.5) Some(Random.nextInt(10000)) else None))
    }

    "correctly convert Seq[Option[Short]] to BytePointerColVector and back" in {
      runConversionTest(0.until(Random.nextInt(100)).map(_ => if (Math.random < 0.5) Some(Random.nextInt(10000).toShort) else None))
    }

    "correctly convert Seq[Option[Long]] to BytePointerColVector and back" in {
      runConversionTest(0.until(Random.nextInt(100)).map(_ => if (Math.random < 0.5) Some(Random.nextLong) else None))
    }

    "correctly convert Seq[Option[Float]] to BytePointerColVector and back" in {
      runConversionTest(0.until(Random.nextInt(100)).map(_ => if (Math.random < 0.5) Some(Random.nextFloat * 1000) else None))
    }

    "correctly convert Seq[Option[Double]] to BytePointerColVector and back" in {
      runConversionTest(0.until(Random.nextInt(100)).map(_ => if (Math.random < 0.5) Some(Random.nextDouble * 1000) else None))
    }

    "correctly convert Seq[Option[String] to BytePointerColVector and back" in {
      val input = 0.until(Random.nextInt(100)).map(_ => if (Math.random < 0.5) Some(Random.nextString(Random.nextInt(30))) else None)

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
        accum + x.map(_.getBytes("UTF-32LE").size).getOrElse(0)
      }
      colvec.underlying.buffers(0).get.capacity() should be (capacity)
      colvec.underlying.buffers(1).get.capacity() should be (input.size.toLong * 4)
      colvec.underlying.buffers(2).get.capacity() should be (input.size.toLong * 4)

      val bitset = FixedBitSet.from(colvec.underlying.buffers(3).get)
      0.until(input.size).foreach(i => bitset.get(i) should be (input(i).nonEmpty))

      // Check conversion - null String values should be preserved as well
      colvec.toSeqOpt[String] should be (input)
    }
  }
}
