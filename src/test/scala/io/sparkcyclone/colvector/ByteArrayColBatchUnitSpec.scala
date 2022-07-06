package io.sparkcyclone.colvector

import io.sparkcyclone.colvector.SeqOptTConversions._
import io.sparkcyclone.colvector.SparkSqlColumnVectorConversions._
import io.sparkcyclone.cache.VeColColumnarVector
import scala.util.Random
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class ByteArrayColBatchUnitSpec extends AnyWordSpec {
  implicit val source = VeColVectorSource(getClass.getName)

  "ByteArrayColBatch" should {
    "correctly enforce input requirements on construction" in {
      val size1 = Random.nextInt(100) + 1
      val size2 = size1 + Random.nextInt(10) + 1

      val column1 = InputSamples.seqOpt[Int](size1).toBytePointerColVector("_").toByteArrayColVector
      val column2 = InputSamples.seqOpt[Float](size1).toBytePointerColVector("_").toByteArrayColVector
      val column3 = InputSamples.seqOpt[Float](size2).toBytePointerColVector("_").toByteArrayColVector

      noException should be thrownBy {
        ByteArrayColBatch(Seq())
        ByteArrayColBatch(Seq(column1))
        ByteArrayColBatch(Seq(column1, column2))
        ByteArrayColBatch(Seq(column3))
      }

      assertThrows[IllegalArgumentException] {
        ByteArrayColBatch(Seq(column1, column3))
      }
    }

    "correctly return the number of rows" in {
      val size = Random.nextInt(100) + 10
      val batch = ByteArrayColBatch(
        Seq(
          InputSamples.seqOpt[Int](size).toBytePointerColVector("_"),
          InputSamples.seqOpt[Short](size).toBytePointerColVector("_"),
          InputSamples.seqOpt[Long](size).toBytePointerColVector("_"),
          InputSamples.seqOpt[Float](size).toBytePointerColVector("_"),
          InputSamples.seqOpt[Double](size).toBytePointerColVector("_"),
          InputSamples.seqOpt[String](size).toBytePointerColVector("_")
        ).map(_.toByteArrayColVector)
      )

      batch.numRows should be (size)
      ByteArrayColBatch(Seq.empty).numRows should be (0)
    }

    "correctly convert to Spark SQL ColumnarBatch" in {
      val size = Random.nextInt(100) + 1
      val columns1 = Seq(
        InputSamples.seqOpt[Int](size).toBytePointerColVector("_"),
        InputSamples.seqOpt[Short](size).toBytePointerColVector("_"),
        InputSamples.seqOpt[Long](size).toBytePointerColVector("_"),
        InputSamples.seqOpt[Float](size).toBytePointerColVector("_"),
        InputSamples.seqOpt[Double](size).toBytePointerColVector("_"),
        InputSamples.seqOpt[String](size).toBytePointerColVector("_")
      ).map(_.toByteArrayColVector)

      val batch = ByteArrayColBatch(columns1).toSparkColumnarBatch
      batch.numCols should be (columns1.size)
      batch.numRows should be (size)

      val columns2 = batch.columns.map(_.asInstanceOf[VeColColumnarVector].dualVeBatch.right.get)
      (columns1, columns2).zipped.map(_ === _).toSet should be (Set(true))
    }
  }
}
