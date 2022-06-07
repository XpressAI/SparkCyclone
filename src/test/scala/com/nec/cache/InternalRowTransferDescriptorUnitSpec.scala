package com.nec.cache

import com.nec.colvector.VeColVectorSource
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.util.FixedBitSet
import com.nec.util.PointerOps._
import com.nec.vectorengine.WithVeProcess
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow, PrettyAttribute}
import org.apache.spark.sql.types.{DoubleType, FloatType, ShortType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.bytedeco.javacpp._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

@VectorEngineTest
final class InternalRowTransferDescriptorUnitSpec extends AnyWordSpec with WithVeProcess {
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

  private def sampleDescriptor: InternalRowTransferDescriptor = {
    val (schema, rows) = batch
    InternalRowTransferDescriptor(schema, rows.toArray)
  }

  "TransferDescriptor" should {
    "correctly calculate column sizes" in {
      val descriptor = sampleDescriptor

      val colSizes = descriptor.colSizes

      colSizes.size should be (4)

      colSizes(0).size should be (2)
      colSizes(1).size should be (2)
      colSizes(2).size should be (2)
      colSizes(3).size should be (4)

      colSizes(0)(0) should be (9 * 4)
      colSizes(0)(1) should be (8)

      colSizes(2)(0) should be (9 * 8)
      colSizes(2)(1) should be (8)

      colSizes(3)(0) should be ("applebananacarrotdurianeggplantfiggrapehawthornichigo".length * 4)
      colSizes(3)(1) should be (9 * 4)
      colSizes(3)(2) should be (9 * 4)
      colSizes(3)(3) should be (8)
    }

    "correctly write the transfer header into the buffer" in {
      val descriptor = sampleDescriptor
      val types = descriptor.colTypes
      val rows = descriptor.rows
      val colSizes = descriptor.colSizes

      // 3 for the top level header, and 4 + 4 + 4 + 6 for the sum of all the column descriptors
      val headersize = (3 + (descriptor.nbatches * (4 + 4 + 4 + 6)))
      val header = descriptor.buffer.as[LongPointer].toArray.take(headersize.toInt)

      descriptor.nbatches should be (1)
      descriptor.ncolumns should be (4)

      header should be (Array[Long](
        // Descriptor header
        headersize * 8, descriptor.nbatches, descriptor.ncolumns,
        // Column headers for the first column of each batch
        types(0).cEnumValue, rows.size, colSizes(0)(0), colSizes(0)(1),
        // Column headers for the second column of each batch
        types(1).cEnumValue, rows.size, colSizes(1)(0), colSizes(1)(1),
        // Column headers for the third column of each batch
        types(2).cEnumValue, rows.size, colSizes(2)(0), colSizes(2)(1),
        // Column headers for the fourth column of each batch
        types(3).cEnumValue, rows.size, colSizes(3)(0), colSizes(3)(1), colSizes(3)(2), colSizes(3)(3)
      ))
    }

    "correctly have vector-aligned (8-byte aligned) data offsets" in {
      val descriptor = sampleDescriptor
      val offsets = descriptor.dataOffsets.flatten

      offsets.foreach { x =>
        x % 8 should be (0)
      }

      offsets.sliding(2).foreach { case x :: y :: _ =>
        (y - x) % 8 should be (0)
      }

      // The generated buffer should 8-byte aligned
      descriptor.buffer.limit() % 8 should be (0)
    }

    "correctly write scalar columns into the data buffers" in {
      val descriptor = sampleDescriptor
      val buffer = descriptor.buffer
      val offsets = descriptor.dataOffsets

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
      val offsets = descriptor.dataOffsets

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
  }
}
