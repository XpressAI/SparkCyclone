package io.sparkcyclone.data.vector

import io.sparkcyclone.data.InputSamples
import io.sparkcyclone.data.conversion.ArrayTConversions._
import io.sparkcyclone.data.conversion.SeqOptTConversions._
import io.sparkcyclone.data.conversion.SparkSqlColumnarBatchConversions._
import io.sparkcyclone.annotations.VectorEngineTest
import io.sparkcyclone.vectorengine.WithVeProcess
import io.sparkcyclone.util.CallContextOps._
import scala.util.Random
import java.io._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

@VectorEngineTest
final class VeColBatchUnitSpec extends AnyWordSpec with WithVeProcess {
  "VeColBatch" should {
    "correctly serialize and deserialize a batch of 2 columns through Array[Byte]" in {
      val size = Random.nextInt(100) + 10
      val input1 = InputSamples.seqOpt[Int](size)
      val input2 = InputSamples.seqOpt[Double](size)
      val input3 = InputSamples.seqOpt[String](size)

      val batch1 = VeColBatch(Seq(
        input1.toBytePointerColVector("_").toVeColVector,
        input2.toBytePointerColVector("_").toVeColVector,
        input3.toBytePointerColVector("_").toVeColVector
      ))
      val batch2 = VeColBatch.fromBytes(batch1.toBytes)

      batch2.columns.size should be (3)
      batch2.columns(0).toBytePointerColVector.toSeqOpt[Int] should be (input1)
      batch2.columns(1).toBytePointerColVector.toSeqOpt[Double] should be (input2)
      batch2.columns(2).toBytePointerColVector.toSeqOpt[String] should be (input3)
    }

    "correctly serialize and deserialize a batch of 2 columns through java.io.OutputStream and java.io.InputStream" in {
      val size = Random.nextInt(100) + 10
      val input1 = InputSamples.seqOpt[Int](size)
      val input2 = InputSamples.seqOpt[Double](size)
      val input3 = InputSamples.seqOpt[String](size)
      val batch1 = VeColBatch(Seq(
        input1.toBytePointerColVector("_").toVeColVector,
        input2.toBytePointerColVector("_").toVeColVector,
        input3.toBytePointerColVector("_").toVeColVector
      ))

      val bostream = new ByteArrayOutputStream
      val ostream = new DataOutputStream(bostream)
      batch1.toStream(ostream)

      val bistream = new ByteArrayInputStream(bostream.toByteArray)
      val istream = new DataInputStream(bistream)
      val batch2 = VeColBatch.fromStream(istream)

      batch2.columns.size should be (3)
      batch2.columns(0).toBytePointerColVector.toSeqOpt[Int] should be (input1)
      batch2.columns(1).toBytePointerColVector.toSeqOpt[Double] should be (input2)
      batch2.columns(2).toBytePointerColVector.toSeqOpt[String] should be (input3)
    }

    "correctly convert to Spark SQL ColumnarBatch as a WrappedColumnarBatch" in {
      val size = Random.nextInt(100) + 1
      val columns1 = Seq(
        InputSamples.seqOpt[Int](size).toBytePointerColVector("_"),
        InputSamples.seqOpt[Short](size).toBytePointerColVector("_"),
        InputSamples.seqOpt[Long](size).toBytePointerColVector("_"),
        InputSamples.seqOpt[Float](size).toBytePointerColVector("_"),
        InputSamples.seqOpt[Double](size).toBytePointerColVector("_"),
        InputSamples.seqOpt[String](size).toBytePointerColVector("_")
      ).map(_.toVeColVector)

      val batch = VeColBatch(columns1).toSparkColumnarBatch
      batch.numCols should be (columns1.size)
      batch.numRows should be (size)

      val columns2 = batch.asInstanceOf[WrappedColumnarBatch[VeColVector]].underlying.columns
      (columns1, columns2).zipped.map(_ === _).toSet should be (Set(true))
    }
  }
}
