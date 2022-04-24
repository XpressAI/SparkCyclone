package com.nec.colvector

import com.nec.colvector.SeqOptTConversions._
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.spark.agile.core.{VeNullableInt, VeString}
import com.nec.ve.WithVeProcess
import scala.util.Random
import java.io._
import java.util.UUID
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

@VectorEngineTest
final class UnitColVectorUnitSpec extends AnyWordSpec with WithVeProcess {
  import com.nec.ve.VeProcess.OriginalCallingContext.Automatic._

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

    s"correctly construct ${classOf[VeColVector].getSimpleName} from Array[Byte] (Int)" in {
      val input = InputSamples.seqOpt[Int]
      val colvec1 = input.toBytePointerColVector("_").toVeColVector
      val colvec2 = colvec1.toUnitColVector.withData(colvec1.toBytes)

      colvec2.toBytePointerColVector.toSeqOpt[Int] should be (input)
    }

    s"correctly construct ${classOf[VeColVector].getSimpleName} from Array[Byte] (String)" in {
      val input = InputSamples.seqOpt[String]
      val colvec1 = input.toBytePointerColVector("_").toVeColVector
      val colvec2 = colvec1.toUnitColVector.withData(colvec1.toBytes)

      colvec2.toBytePointerColVector.toSeqOpt[String] should be (input)
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
