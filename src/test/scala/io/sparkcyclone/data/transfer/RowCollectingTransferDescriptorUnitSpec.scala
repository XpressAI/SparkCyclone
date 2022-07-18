package io.sparkcyclone.data.transfer

import io.sparkcyclone.data._
import io.sparkcyclone.data.conversion.SeqOptTConversions._
import io.sparkcyclone.data.vector.BytePointerColBatch
import io.sparkcyclone.annotations.VectorEngineTest
import io.sparkcyclone.util.FixedBitSet
import io.sparkcyclone.util.PointerOps._
import io.sparkcyclone.vectorengine.WithVeProcess
import scala.util.Random
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow, PrettyAttribute}
import org.apache.spark.sql.types.{DoubleType, FloatType, ShortType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.bytedeco.javacpp._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

@VectorEngineTest
final class RowCollectingTransferDescriptorUnitSpec extends AnyWordSpec with WithVeProcess {
  private def batch: (Seq[Attribute], List[InternalRow]) = {
    val schema = Seq(
      PrettyAttribute("short", ShortType),
      PrettyAttribute("float", FloatType),
      PrettyAttribute("double", DoubleType),
      PrettyAttribute("string", StringType)
    )

    val data = List(
      new GenericInternalRow(Seq(1.toShort, 10.toFloat, 100.toDouble, UTF8String.fromString("apple")).toArray),
      new GenericInternalRow(Seq(2.toShort, 20.toFloat, 200.toDouble, UTF8String.fromString("banana")).toArray),
      new GenericInternalRow(Seq(3.toShort, 30.toFloat, 300.toDouble, UTF8String.fromString("carrot")).toArray),
      new GenericInternalRow(Seq(4.toShort, 40.toFloat, 400.toDouble, UTF8String.fromString("durian")).toArray),
      new GenericInternalRow(Seq(5.toShort, 50.toFloat, 500.toDouble, UTF8String.fromString("eggplant")).toArray),
      new GenericInternalRow(Seq(6.toShort, 60.toFloat, 600.toDouble, UTF8String.fromString("fig")).toArray),
      new GenericInternalRow(Seq(7.toShort, 70.toFloat, 700.toDouble, UTF8String.fromString("grape")).toArray),
      new GenericInternalRow(Seq(8.toShort, 80.toFloat, 800.toDouble, UTF8String.fromString("hawthorn")).toArray),
      new GenericInternalRow(Seq(9.toShort, 90.toFloat, 900.toDouble, UTF8String.fromString("ichigo")).toArray),
    )

    (schema, data)
  }

  private def sampleDescriptor: RowCollectingTransferDescriptor = {
    val (schema, rows) = batch
    val descriptor = RowCollectingTransferDescriptor(schema, 9)
    rows.foreach(row => descriptor.append(row))
    descriptor
  }

  // 3 for the top level header, and 4 + 4 + 4 + 6 for the sum of all the column descriptors
  val headerSize = (3 + 4 + 4 + 4 + 6)

  val colSizes = Seq(
    Seq(9 * 4, 8),
    Seq(9 * 4, 8),
    Seq(9 * 8, 8),
    Seq("applebananacarrotdurianeggplantfiggrapehawthornichigo".length * 4, 9 * 4, 9 * 4, 8)
  )

  val dataOffsets = Seq(
    // 168 = Data start offset
    List(168, 208, 216),
    List(216, 256, 264),
    List(264, 336, 344),
    List(344, 560, 600, 640, 648)
  )

  val expectedRows = 9

  "RowCollectingTransferDescriptor" should {
    "correctly allocates buffer" in {
      val descriptor = sampleDescriptor
      val expectedSize = dataOffsets.last.last
      descriptor.buffer.limit() should be(expectedSize)
    }

    "correctly write the transfer header into the buffer" in {
      val descriptor = sampleDescriptor
      val types = descriptor.transferCols.map(_.veType)

      val header = descriptor.buffer.as[LongPointer].toArray.take(headerSize)

      header should be (Array[Long](
        // Descriptor header
        headerSize * 8, 1, 4,
        // Column headers for the first column of each batch
        types(0).cEnumValue, expectedRows, colSizes(0)(0), colSizes(0)(1),
        // Column headers for the second column of each batch
        types(1).cEnumValue, expectedRows, colSizes(1)(0), colSizes(1)(1),
        // Column headers for the third column of each batch
        types(2).cEnumValue, expectedRows, colSizes(2)(0), colSizes(2)(1),
        // Column headers for the fourth column of each batch
        types(3).cEnumValue, expectedRows, colSizes(3)(0), colSizes(3)(1), colSizes(3)(2), colSizes(3)(3)
      ))
    }

    "correctly write scalar columns into the data buffers" in {
      val descriptor = sampleDescriptor
      val buffer = descriptor.buffer
      val offsets = dataOffsets

      // Column A data should be correctly written
      buffer.slice(offsets(0)(0), offsets(0)(1) - offsets(0)(0)).as[IntPointer].toArray.map(_.toShort) should be (Array[Short](1, 2, 3, 4, 5, 6, 7, 8, 9, 0))
      // Column A validity buffers should be correctly written
      buffer.slice(offsets(0)(1), offsets(0)(2) - offsets(0)(1)).toArray should be (FixedBitSet.ones(9).toByteArray)

      // Column B data should be correctly written
      buffer.slice(offsets(1)(0), offsets(1)(1) - offsets(1)(0)).as[FloatPointer].toArray should be (Array[Float](10, 20, 30, 40, 50, 60, 70, 80, 90, 0))
      // Column B validity buffers should be correctly written
      buffer.slice(offsets(1)(1), offsets(1)(2) - offsets(1)(1)).toArray should be (FixedBitSet.ones(9).toByteArray)

      // Column C data should be correctly written
      buffer.slice(offsets(2)(0), offsets(2)(1) - offsets(2)(0)).as[DoublePointer].toArray should be (Array[Double](100, 200, 300, 400, 500, 600, 700, 800, 900))
      // Column C validity buffers should be correctly written
      buffer.slice(offsets(2)(1), offsets(2)(2) - offsets(2)(1)).toArray should be (FixedBitSet.ones(9).toByteArray)
    }

    "correctly write varchar columns into the data buffers" in {
      val descriptor = sampleDescriptor
      val buffer = descriptor.buffer
      val offsets = dataOffsets

      val expectedStrings = Array("apple", "banana", "carrot", "durian", "eggplant", "fig", "grape", "hawthorn", "ichigo")
      val expectedStringBytes = expectedStrings.map(_.getBytes("UTF-32LE")).flatten ++ Array[Byte](0, 0, 0, 0)

      // Column D data should be correctly written
      buffer.slice(offsets(3)(0), offsets(3)(1) - offsets(3)(0)).toArray should be (expectedStringBytes)

      // Column D offsets should be correctly written
      buffer.slice(offsets(3)(1), offsets(3)(2) - offsets(3)(1)).as[IntPointer].toArray should be (Array(0, 5, 11, 17, 23, 31, 34, 39, 47, 0))

      // Column D lens should be correctly written
      buffer.slice(offsets(3)(2), offsets(3)(3) - offsets(3)(2)).as[IntPointer].toArray should be ((expectedStrings.map(_.length) ++ Array(0)))

      // Column D validity buffers should be correctly written
      buffer.slice(offsets(3)(3), offsets(3)(4) - offsets(3)(3)).toArray should be (FixedBitSet.ones(9).toByteArray)
    }

    "correctly read the result buffer" in {
      val descriptor = sampleDescriptor
      val buffer = descriptor.resultBuffer

      buffer.limit() should be (3 + 3 + 3 + 5)

      // put up some imaginary values
      buffer
        // Col A
        .put(0, 1)
        .put(1, 2)
        .put(2, 3)
        // Col B
        .put(3, 4)
        .put(4, 5)
        .put(5, 6)
        // Col C
        .put(6, 7)
        .put(7, 8)
        .put(8, 9)
        // Col D
        .put(9, 10)
        .put(10, 11)
        .put(11, 12)
        .put(12, 13)
        .put(13, 14)

      val batch = descriptor.resultToColBatch(VeColVectorSource("test"))
      batch.numRows should be (9)
      batch.columns.size should be (4)

      val col1 = batch.columns(0)
      val col2 = batch.columns(1)
      val col3 = batch.columns(2)
      val col4 = batch.columns(3)

      col1.numItems should be (9)
      col2.numItems should be (9)
      col3.numItems should be (9)
      col4.numItems should be (9)

      col1.container should be (1)
      col1.buffers should be (Seq(2, 3))

      col2.container should be (4)
      col2.buffers should be (Seq(5, 6))

      col3.container should be (7)
      col3.buffers should be (Seq(8, 9))

      col4.container should be (10)
      col4.buffers should be (Seq(11, 12, 13, 14))
    }

    "correctly generate a BytePointerColBatch" in {
      val size = Random.nextInt(50) + 10
      val x1 = InputSamples.seqOpt[Int](size)
      val x2 = InputSamples.seqOpt[Double](size)
      val x3 = InputSamples.seqOpt[String](size)

      val batch1 = BytePointerColBatch(Seq(
        x1.toBytePointerColVector("_"),
        x2.toBytePointerColVector("_"),
        x3.toBytePointerColVector("_")
      ))

      val batch2 = batch1.internalRowIterator
        .foldLeft(RowCollectingTransferDescriptor(batch1.sparkAttributes, size)) { case (accum, row) =>
          accum.append(row)
          accum
        }
        .toBytePointerColBatch

      batch2.numCols should be (batch1.numCols)
      batch2.columns(0).toSeqOpt[Int] should be (x1)
      batch2.columns(1).toSeqOpt[Double] should be (x2)
      batch2.columns(2).toSeqOpt[String] should be (x3)
    }
  }
}
