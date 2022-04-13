package com.nec.arrow.colvector

import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.nec.arrow.colvector.SeqOptTConversions._
import scala.reflect.ClassTag
import scala.util.Random
import java.util.UUID
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import com.nec.spark.agile.core.VeNullableInt
import com.nec.spark.agile.core.VeString

class ByteArrayColVectorUnitSpec extends AnyWordSpec {
  def runConversionTest[T <: AnyVal : ClassTag](input: Seq[Option[T]]): Unit = {
    implicit val source = VeColVectorSource(s"${UUID.randomUUID}")
    val name = s"${UUID.randomUUID}"
    val colvec = input.toBytePointerColVector(name).toByteArrayColVector

    // Check fields
    colvec.name should be (name)
    colvec.source should be (source)
    colvec.numItems should be (input.size)
    colvec.veType.scalaType should be (implicitly[ClassTag[T]].runtimeClass)
    colvec.buffers.size should be (2)
    colvec.toBytePointerColVector.underlying.variableSize shouldBe empty

    // Check conversion
    colvec.toBytePointerColVector.toSeqOpt[T] should be (input)
  }

  "ByteArrayColVector" should {
    "correctly convert from and to BytePointerColVector (Int)" in {
      runConversionTest(0.until(Random.nextInt(100)).map(_ => if (Math.random < 0.5) Some(Random.nextInt(10000)) else None))
    }

    "correctly convert from and to BytePointerColVector (Short)" in {
      runConversionTest(0.until(Random.nextInt(100)).map(_ => if (Math.random < 0.5) Some(Random.nextInt(10000).toShort) else None))
    }

    "correctly convert from and to BytePointerColVector (Long)" in {
      runConversionTest(0.until(Random.nextInt(100)).map(_ => if (Math.random < 0.5) Some(Random.nextLong) else None))
    }

    "correctly convert from and to BytePointerColVector (Float)" in {
      runConversionTest(0.until(Random.nextInt(100)).map(_ => if (Math.random < 0.5) Some(Random.nextFloat * 1000) else None))
    }

    "correctly convert from and to BytePointerColVector (Double)" in {
      runConversionTest(0.until(Random.nextInt(100)).map(_ => if (Math.random < 0.5) Some(Random.nextDouble * 1000) else None))
    }

    "correctly convert from and to BytePointerColVector (String)" in {
      val input = 0.until(Random.nextInt(100)).map(_ => if (Math.random < 0.5) Some(Random.nextString(Random.nextInt(30))) else None)

      val source = VeColVectorSource(s"${UUID.randomUUID}")
      val name = s"${UUID.randomUUID}"
      val colvec = input.toBytePointerColVector(name)(source).toByteArrayColVector

      // Check fields
      colvec.name should be (name)
      colvec.source should be (source)
      colvec.numItems should be (input.size)
      colvec.veType.scalaType should be (classOf[String])
      colvec.buffers.size should be (4)
      colvec.toBytePointerColVector.underlying.variableSize should be (Some(colvec.buffers.head.size / 4))

      // Check conversion
      colvec.toBytePointerColVector.toSeqOpt[String] should be (input)
    }

    "correctly enforce input requirements on construction" in {
      val source = VeColVectorSource(s"${UUID.randomUUID}")
      val name = s"${UUID.randomUUID}"

      assertThrows[IllegalArgumentException] {
        ByteArrayColVector(source, Random.nextInt(100), name, VeNullableInt, Seq.empty[Array[Byte]])
      }

      assertThrows[IllegalArgumentException] {
        ByteArrayColVector(
          source,
          Random.nextInt(100),
          name,
          VeNullableInt,
          Seq(Array.empty[Byte], Array.empty[Byte], Array.empty[Byte], Array.empty[Byte])
        )
      }

      noException should be thrownBy {
        ByteArrayColVector(
          source,
          Random.nextInt(100),
          name,
          VeNullableInt,
          Seq(
            Random.nextString(Random.nextInt(100) + 1).getBytes,
            Random.nextString(Random.nextInt(100) + 1).getBytes
          )
        )
      }
    }

    "be constructable from empty column vectors" in {
      implicit val source = VeColVectorSource(s"${UUID.randomUUID}")
      val name = s"${UUID.randomUUID}"

      noException should be thrownBy { Seq.empty[Option[Int]].toBytePointerColVector(name).toByteArrayColVector }
      noException should be thrownBy { Seq.empty[Option[Short]].toBytePointerColVector(name).toByteArrayColVector }
      noException should be thrownBy { Seq.empty[Option[Long]].toBytePointerColVector(name).toByteArrayColVector }
      noException should be thrownBy { Seq.empty[Option[Float]].toBytePointerColVector(name).toByteArrayColVector }
      noException should be thrownBy { Seq.empty[Option[Double]].toBytePointerColVector(name).toByteArrayColVector }
      noException should be thrownBy { Seq.empty[Option[String]].toBytePointerColVector(name).toByteArrayColVector }
    }

    "correctly serialize to Array[Byte]" in {
      val buffers = Seq(
        Random.nextString(Random.nextInt(100) + 1).getBytes,
        Random.nextString(Random.nextInt(100) + 1).getBytes,
        Random.nextString(Random.nextInt(100) + 1).getBytes,
        Random.nextString(Random.nextInt(100) + 1).getBytes
      )
      val colvec = ByteArrayColVector(VeColVectorSource(s"${UUID.randomUUID}"), Random.nextInt(100), s"${UUID.randomUUID}", VeString, buffers)

      colvec.serialize.toSeq should be (buffers.foldLeft(Array.empty[Byte])(_ ++ _).toSeq)
    }
  }
}
