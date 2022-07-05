package io.sparkcyclone.colvector

import io.sparkcyclone.colvector.SeqOptTConversions._
import io.sparkcyclone.util.CallContextOps._
import scala.util.Random
import java.util.UUID
import java.io._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class BytePointerColBatchUnitSpec extends AnyWordSpec {
  implicit val source = VeColVectorSource(s"${UUID.randomUUID}")

  "BytePointerColBatch" should {
    "correctly construct an Iterator[InternalRow]" in {
      val size = Random.nextInt(100) + 10
      val input1 = InputSamples.seqOpt[Int](size)
      val input2 = InputSamples.seqOpt[Double](size)
      val input3 = InputSamples.seqOpt[String](size)

      val batch = BytePointerColBatch(Seq(
        input1.toBytePointerColVector("_"),
        input2.toBytePointerColVector("_"),
        input3.toBytePointerColVector("_")
      ))

      val expected = Seq(input1, input2, input3)
        .map(_.map(_.getOrElse(null)))
        .transpose

      /*
        Loop multiple times to test that a new working Iterator[InternalRow]
        is generated each time
      */
      0.to(Random.nextInt(4) + 1).foreach { _ =>
        val output = batch.internalRowIterator
          .map(_.toSeq(batch.sparkSchema).toSeq)
          // Don't use toList on Iterator[T] !!!
          .toSeq
          .map { arr =>
            arr.map {
              case x: UTF8String => x.toString
              case x => x
            }
          }

        output should be (expected)
      }
    }
  }
}
