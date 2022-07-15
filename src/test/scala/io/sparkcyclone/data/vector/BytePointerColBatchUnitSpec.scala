package io.sparkcyclone.data.vector

import io.sparkcyclone.data.conversion.SeqOptTConversions._
import io.sparkcyclone.data.conversion.SparkSqlColumnarBatchConversions._
import io.sparkcyclone.data.{InputSamples, VeColVectorSource}
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

    "correctly convert to Spark SQL ColumnarBatch with WrappedColumnVector" in {
      val size = Random.nextInt(100) + 1
      val columns1 = Seq(
        InputSamples.seqOpt[Int](size).toBytePointerColVector("_"),
        InputSamples.seqOpt[Short](size).toBytePointerColVector("_"),
        InputSamples.seqOpt[Long](size).toBytePointerColVector("_"),
        InputSamples.seqOpt[Float](size).toBytePointerColVector("_"),
        InputSamples.seqOpt[Double](size).toBytePointerColVector("_"),
        InputSamples.seqOpt[String](size).toBytePointerColVector("_")
      )

      val batch = BytePointerColBatch(columns1).toSparkColumnarBatch
      batch.numCols should be (columns1.size)
      batch.numRows should be (size)

      val columns2 = batch.columns.map(_.asInstanceOf[WrappedColumnVector].underlying.asInstanceOf[WrappedColumnVector.BP].vector)
      (columns1, columns2).zipped.map(_ === _).toSet should be (Set(true))
    }
  }
}
