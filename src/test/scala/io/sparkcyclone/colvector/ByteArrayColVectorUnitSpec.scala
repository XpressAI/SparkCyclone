package io.sparkcyclone.colvector

import io.sparkcyclone.colvector.SeqOptTConversions._
import scala.reflect.ClassTag
import scala.util.Random
import java.util.UUID
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import io.sparkcyclone.spark.agile.core.VeNullableInt
import io.sparkcyclone.spark.agile.core.VeString

final class ByteArrayColVectorUnitSpec extends AnyWordSpec {
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
    colvec.toBytePointerColVector.dataSize shouldBe empty

    // Check equality
    (colvec === input.toBytePointerColVector(name).toByteArrayColVector) should be (true)

    // Check conversion
    colvec.toBytePointerColVector.toSeqOpt[T] should be (input)
  }

  "ByteArrayColVector" should {
    "correctly convert from and to BytePointerColVector (Int)" in {
      runConversionTest(InputSamples.seqOpt[Int])
    }

    "correctly convert from and to BytePointerColVector (Short)" in {
      runConversionTest(InputSamples.seqOpt[Short])
    }

    "correctly convert from and to BytePointerColVector (Long)" in {
      runConversionTest(InputSamples.seqOpt[Long])
    }

    "correctly convert from and to BytePointerColVector (Float)" in {
      runConversionTest(InputSamples.seqOpt[Float])
    }

    "correctly convert from and to BytePointerColVector (Double)" in {
      runConversionTest(InputSamples.seqOpt[Double])
    }

    "correctly convert from and to BytePointerColVector (String)" in {
      val input = InputSamples.seqOpt[String]

      val source = VeColVectorSource(s"${UUID.randomUUID}")
      val name = s"${UUID.randomUUID}"
      val colvec = input.toBytePointerColVector(name)(source).toByteArrayColVector

      // Check fields
      colvec.name should be (name)
      colvec.source should be (source)
      colvec.numItems should be (input.size)
      colvec.veType.scalaType should be (classOf[String])
      colvec.buffers.size should be (4)
      colvec.toBytePointerColVector.dataSize should be (Some(colvec.buffers.head.size / 4))

      // Check conversion
      colvec.toBytePointerColVector.toSeqOpt[String] should be (input)
    }

    "correctly enforce input requirements on construction" in {
      val source = VeColVectorSource(s"${UUID.randomUUID}")
      val name = s"${UUID.randomUUID}"

      assertThrows[IllegalArgumentException] {
        ByteArrayColVector(source, name, VeNullableInt, Random.nextInt(100), Seq.empty[Array[Byte]])
      }

      assertThrows[IllegalArgumentException] {
        ByteArrayColVector(
          source,
          name,
          VeNullableInt,
          Random.nextInt(100),
          Seq(Array.empty[Byte], Array.empty[Byte], Array.empty[Byte], Array.empty[Byte])
        )
      }

      noException should be thrownBy {
        ByteArrayColVector(
          source,
          name,
          VeNullableInt,
          Random.nextInt(100),
          Seq(
            Random.nextString(Random.nextInt(100) + 100).getBytes,
            Random.nextString(Random.nextInt(100) + 100).getBytes
          )
        )
      }
    }

    "correctly perform value equality" in {
      implicit val source = VeColVectorSource(s"${UUID.randomUUID}")

      val input1 = InputSamples.seqOpt[Int]
      val input2 = InputSamples.seqOpt[Double]
      val input3 = InputSamples.seqOpt[String]

      input1.toBytePointerColVector("_").toByteArrayColVector === input1.toBytePointerColVector("_").toByteArrayColVector should be (true)
      input2.toBytePointerColVector("_").toByteArrayColVector === input2.toBytePointerColVector("_").toByteArrayColVector should be (true)
      input3.toBytePointerColVector("_").toByteArrayColVector === input3.toBytePointerColVector("_").toByteArrayColVector should be (true)
    }

    "be constructable from empty column vectors" in {
      implicit val source = VeColVectorSource(s"${UUID.randomUUID}")

      noException should be thrownBy { Seq.empty[Option[Int]].toBytePointerColVector("_").toByteArrayColVector }
      noException should be thrownBy { Seq.empty[Option[Short]].toBytePointerColVector("_").toByteArrayColVector }
      noException should be thrownBy { Seq.empty[Option[Long]].toBytePointerColVector("_").toByteArrayColVector }
      noException should be thrownBy { Seq.empty[Option[Float]].toBytePointerColVector("_").toByteArrayColVector }
      noException should be thrownBy { Seq.empty[Option[Double]].toBytePointerColVector("_").toByteArrayColVector }
      noException should be thrownBy { Seq.empty[Option[String]].toBytePointerColVector("_").toByteArrayColVector }
    }
  }
}
