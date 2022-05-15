package com.nec.colvector

import com.nec.colvector.SeqOptTConversions._
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.spark.agile.core.{VeNullableInt, VeString}
import com.nec.util.CallContextOps._
import com.nec.vectorengine.WithVeProcess
import scala.util.Random
import java.io._
import java.util.UUID
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

@VectorEngineTest
final class UnitColVectorUnitSpec extends AnyWordSpec with WithVeProcess {
  def runSerializationTest(input: BytePointerColVector): BytePointerColVector = {
    val colvec1 = input.toVeColVector
    val bytes = colvec1.toBytes
    val colvec2 = colvec1.toUnitColVector.withData(bytes).apply().get

    colvec1.container should not be (colvec2.container)
    colvec1.buffers should not be (colvec2.buffers)
    colvec2.toBytes.toSeq should be (bytes.toSeq)

    colvec2.toBytePointerColVector
  }

  "UnitColVector" should {
    "correctly enforce input requirements on construction" in {
      val source = VeColVectorSource(s"${UUID.randomUUID}")
      val name = s"${UUID.randomUUID}"

      assertThrows[IllegalArgumentException] {
        // Negative numItems
        UnitColVector(source, name, VeNullableInt, - (Random.nextInt(100) + 1), None)
      }

      assertThrows[IllegalArgumentException] {
        // dataSizeO is empty for VeString
        UnitColVector(source, name, VeString, Random.nextInt(100), None)
      }

      noException should be thrownBy {
        UnitColVector(source, name, VeNullableInt, Random.nextInt(100), None)
        UnitColVector(source, name, VeString, Random.nextInt(100), Some(Random.nextInt(100)))
      }
    }

    "correctly serialize to and deserialize from Array[Byte] (Int)" in {
      val input = InputSamples.seqOpt[Int]
      runSerializationTest(input.toBytePointerColVector("_")).toSeqOpt[Int] should be (input)
    }

    "correctly serialize to and deserialize from Array[Byte] (Short)" in {
      val input = InputSamples.seqOpt[Short]
      runSerializationTest(input.toBytePointerColVector("_")).toSeqOpt[Short] should be (input)
    }

    "correctly serialize to and deserialize from Array[Byte] (Long)" in {
      val input = InputSamples.seqOpt[Long]
      runSerializationTest(input.toBytePointerColVector("_")).toSeqOpt[Long] should be (input)
    }

    "correctly serialize to and deserialize from Array[Byte] (Float)" in {
      val input = InputSamples.seqOpt[Float]
      runSerializationTest(input.toBytePointerColVector("_")).toSeqOpt[Float] should be (input)
    }

    "correctly serialize to and deserialize from Array[Byte] (Double)" in {
      val input = InputSamples.seqOpt[Double]
      runSerializationTest(input.toBytePointerColVector("_")).toSeqOpt[Double] should be (input)
    }

    "correctly serialize to and deserialize from Array[Byte] (String)" in {
      val input = InputSamples.seqOpt[String]
      runSerializationTest(input.toBytePointerColVector("_")).toSeqOpt[String] should be (input)
    }

    "correctly serialize to java.io.OutputStream and deserialize from java.io.InputStream" in {
      val colvec1 = UnitColVector(
        VeColVectorSource("tested"),
        "test",
        VeString,
        9,
        Some(123)
      )

      val ostream = new ByteArrayOutputStream
      val output = new DataOutputStream(ostream)
      colvec1.toStream(output)

      val bytes = ostream.toByteArray
      val istream = new ByteArrayInputStream(bytes)
      val input = new DataInputStream(istream)
      val colvec2 = UnitColVector.fromStream(input)

      colvec1 should be (colvec2)
      bytes.length should be (colvec1.streamedSize)
    }
  }
}
