package com.nec.colvector

import com.nec.colvector.SeqOptTConversions._
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.ve.{VeKernelInfra, WithVeProcess}
import scala.util.Random
import java.io._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

@VectorEngineTest
final class VeColVectorUnitSpec extends AnyWordSpec with WithVeProcess {
  import com.nec.ve.VeProcess.OriginalCallingContext.Automatic._

  def runByteArraySerializationTest(input: BytePointerColVector): BytePointerColVector = {
    val colvec1 = input.toVeColVector
    val bytes = colvec1.toBytes
    val colvec2 = colvec1.toUnitColVector.withData(bytes)

    colvec1.container should not be (colvec2.container)
    colvec1.buffers should not be (colvec2.buffers)
    colvec2.toBytes.toSeq should be (bytes.toSeq)

    colvec2.toBytePointerColVector
  }

  "VeColVector" should {
    "correctly serialize to and deserialize from Array[Byte] (Int)" in {
      val input = InputSamples.seqOpt[Int]
      runByteArraySerializationTest(input.toBytePointerColVector("_")).toSeqOpt[Int] should be (input)
    }

    "correctly serialize to and deserialize from Array[Byte] (Short)" in {
      val input = InputSamples.seqOpt[Short]
      runByteArraySerializationTest(input.toBytePointerColVector("_")).toSeqOpt[Short] should be (input)
    }

    "correctly serialize to and deserialize from Array[Byte] (Long)" in {
      val input = InputSamples.seqOpt[Long]
      runByteArraySerializationTest(input.toBytePointerColVector("_")).toSeqOpt[Long] should be (input)
    }

    "correctly serialize to and deserialize from Array[Byte] (Float)" in {
      val input = InputSamples.seqOpt[Float]
      runByteArraySerializationTest(input.toBytePointerColVector("_")).toSeqOpt[Float] should be (input)
    }

    "correctly serialize to and deserialize from Array[Byte] (Double)" in {
      val input = InputSamples.seqOpt[Double]
      runByteArraySerializationTest(input.toBytePointerColVector("_")).toSeqOpt[Double] should be (input)
    }

    "correctly serialize to and deserialize from Array[Byte] (String)" in {
      val input = InputSamples.seqOpt[String]
      runByteArraySerializationTest(input.toBytePointerColVector("_")).toSeqOpt[String] should be (input)
    }

    "NOT crash if a double-free were called" in {
      val colvec1 = InputSamples.seqOpt[Int].toBytePointerColVector("_").toVeColVector
      val colvec2 = InputSamples.seqOpt[Double].toBytePointerColVector("_").toVeColVector
      val colvec3 = InputSamples.seqOpt[String].toBytePointerColVector("_").toVeColVector

      noException should be thrownBy {
        // Should call at least twice
        0.to(Random.nextInt(3) + 1).foreach(_ => colvec1.free)
        0.to(Random.nextInt(3) + 1).foreach(_ => colvec2.free)
        0.to(Random.nextInt(3) + 1).foreach(_ => colvec3.free)
      }
    }
  }
}
