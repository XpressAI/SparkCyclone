package com.nec.colvector

import com.eed3si9n.expecty.Expecty.expect
import com.nec.colvector.UnitColVector
import com.nec.spark.agile.core.{VeNullableInt, VeString}
import com.nec.colvector.VeColVectorSource

import scala.util.Random
import java.io._
import java.util.UUID
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class UnitColVectorSpec extends AnyWordSpec {
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
