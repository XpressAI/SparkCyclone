package com.nec.colvector

import com.nec.colvector.SeqOptTConversions._
import com.nec.spark.agile.core.{VeNullableInt, VeString}
import scala.util.Random
import java.util.UUID
import java.io._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import org.bytedeco.javacpp.BytePointer

final class BytePointerColVectorUnitSpec extends AnyWordSpec {
  import com.nec.util.CallContextOps._
  implicit val source = VeColVectorSource(s"${UUID.randomUUID}")

  "BytePointerColVector" should {
    "correctly enforce input requirements on construction" in {
      val name = s"${UUID.randomUUID}"

      assertThrows[IllegalArgumentException] {
        // Negative numItems
        BytePointerColVector(source, name, VeNullableInt, -1, Seq.empty[BytePointer])
      }

      assertThrows[IllegalArgumentException] {
        // Not enough buffers
        BytePointerColVector(source, name, VeNullableInt, 1, Seq(new BytePointer))
      }

      noException should be thrownBy {
        BytePointerColVector(source, name, VeNullableInt, Random.nextInt(100), 0.until(2).map(_ => new BytePointer))
        BytePointerColVector(source, name, VeString, Random.nextInt(100), 0.until(4).map(_ => new BytePointer))
      }
    }

    "convert to ByteArrayColVector and back" in {
      val input1 = InputSamples.seqOpt[Int]
      val input2 = InputSamples.seqOpt[Double]
      val input3 = InputSamples.seqOpt[String]

      input1.toBytePointerColVector("_").toByteArrayColVector.toBytePointerColVector.toSeqOpt[Int] should be (input1)
      input2.toBytePointerColVector("_").toByteArrayColVector.toBytePointerColVector.toSeqOpt[Double] should be (input2)
      input3.toBytePointerColVector("_").toByteArrayColVector.toBytePointerColVector.toSeqOpt[String] should be (input3)
    }

    "correctly serialize to Array[Byte]" in {
      val colvec1 = InputSamples.seqOpt[Int].toBytePointerColVector("_")
      val colvec2 = InputSamples.seqOpt[Double].toBytePointerColVector("_")
      val colvec3 = InputSamples.seqOpt[String].toBytePointerColVector("_")

      colvec1.toBytes.toSeq should be (colvec1.toByteArrayColVector.buffers.flatten)
      colvec2.toBytes.toSeq should be (colvec2.toByteArrayColVector.buffers.flatten)
      colvec3.toBytes.toSeq should be (colvec3.toByteArrayColVector.buffers.flatten)
    }

    "correctly return the dataSize" in {
      InputSamples.seqOpt[Int].toBytePointerColVector("_").dataSize shouldBe empty
      InputSamples.seqOpt[Double].toBytePointerColVector("_").dataSize shouldBe empty

      val input = InputSamples.seqOpt[String]
      val size = input.foldLeft(0) { case (accum, x) =>
        accum + x.map(_.getBytes("UTF-32LE").size).getOrElse(0)
      }

      input.toBytePointerColVector("_").dataSize should be (Some(size / 4))
    }
  }
}
