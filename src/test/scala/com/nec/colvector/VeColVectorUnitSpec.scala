package com.nec.colvector

import com.nec.colvector.SeqOptTConversions._
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.ve.{VeKernelInfra, WithVeProcess}
import java.io._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

@VectorEngineTest
final class VeColVectorUnitSpec extends AnyWordSpec with WithVeProcess {
  import com.nec.ve.VeProcess.OriginalCallingContext.Automatic._

  def runByteArraySerializationTest(input: BytePointerColVector): BytePointerColVector = {
    val colvec1 = input.toVeColVector
    val serialized = colvec1.serialize
    val colvec2 = colvec1.toUnitColVector.withData(serialized)

    colvec1.container should not be (colvec2.container)
    colvec1.buffers should not be (colvec2.buffers)
    colvec2.serialize.toSeq should be (serialized.toSeq)

    colvec2.toBytePointerColVector
  }

  "VeColVector" should {
    "serialize to and deserialize from Array[Byte] (Int)" in {
      val input = InputSamples.seqOpt[Int]
      runByteArraySerializationTest(input.toBytePointerColVector("input")).toSeqOpt[Int] should be (input)
    }

    "serialize to and deserialize from Array[Byte] (Short)" in {
      val input = InputSamples.seqOpt[Short]
      runByteArraySerializationTest(input.toBytePointerColVector("input")).toSeqOpt[Short] should be (input)
    }

    "serialize to and deserialize from Array[Byte] (Long)" in {
      val input = InputSamples.seqOpt[Long]
      runByteArraySerializationTest(input.toBytePointerColVector("input")).toSeqOpt[Long] should be (input)
    }

    "serialize to and deserialize from Array[Byte] (Float)" in {
      val input = InputSamples.seqOpt[Float]
      runByteArraySerializationTest(input.toBytePointerColVector("input")).toSeqOpt[Float] should be (input)
    }

    "serialize to and deserialize from Array[Byte] (Double)" in {
      val input = InputSamples.seqOpt[Double]
      runByteArraySerializationTest(input.toBytePointerColVector("input")).toSeqOpt[Double] should be (input)
    }

    "serialize to and deserialize from Array[Byte] (String)" in {
      val input = InputSamples.seqOpt[String]
      runByteArraySerializationTest(input.toBytePointerColVector("input")).toSeqOpt[String] should be (input)
    }
  }
}
