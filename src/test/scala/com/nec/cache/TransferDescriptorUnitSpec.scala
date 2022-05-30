package com.nec.cache

import com.nec.colvector.BytePointerColVector
import com.nec.colvector.ArrayTConversions._
import com.nec.colvector.SeqOptTConversions._
import com.nec.util.CallContextOps._
import com.nec.util.FixedBitSet
import com.nec.util.PointerOps._
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.vectorengine.{LibCyclone, WithVeProcess}
import org.bytedeco.javacpp._
import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers._
import org.scalatest.prop.TableDrivenPropertyChecks.whenever
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks.forAll
import scala.reflect.ClassTag

@VectorEngineTest
final class TransferDescriptorUnitSpec extends AnyWordSpec with WithVeProcess {
  private def batch1: Seq[BytePointerColVector] = {
    Seq(
      Array[Short](1, 2).toBytePointerColVector("1a"),
      Array[Float](10, 20).toBytePointerColVector("1b"),
      Array[Double](100, 200).toBytePointerColVector("1c"),
      Array[String]("apple", "banana").toBytePointerColVector("1d")
    )
  }

  private def batch2: Seq[BytePointerColVector] = {
    Seq(
      Array[Short](3, 4, 5).toBytePointerColVector("2a"),
      Array[Float](30, 40, 50).toBytePointerColVector("2b"),
      Array[Double](300, 400, 500).toBytePointerColVector("2c"),
      Array[String]("carrot", "durian", "eggplant").toBytePointerColVector("2d")
    )
  }

  private def batch3: Seq[BytePointerColVector] = {
    Seq(
      Array[Short](6, 7, 8, 9).toBytePointerColVector("3a"),
      Array[Float](60, 70, 80, 90).toBytePointerColVector("3b"),
      Array[Double](600, 700, 800, 900).toBytePointerColVector("3c"),
      Array[String]("fig", "grape", "hawthorn", "ichigo").toBytePointerColVector("3d")
    )
  }

  private def sampleDescriptor: TransferDescriptor = {
    TransferDescriptor(Seq(batch1, batch2, batch3))
  }

  "TransferDescriptor" should {
    "correctly write the transfer header into the buffer" in {
      val descriptor = sampleDescriptor

      // 3 for the top level header, and 4 + 4 + 4 + 6 for the sum of all the column descriptors
      val headersize = (3 + (descriptor.nbatches * (4 + 4 + 4 + 6)))
      val header = descriptor.buffer.as[LongPointer].toArray.take(headersize.toInt)

      descriptor.nbatches should be (3)
      descriptor.ncolumns should be (4)

      header should be (Array[Long](
        // Descriptor header
        headersize * 8, descriptor.nbatches, descriptor.ncolumns,
        // Column headers for the first column of each batch
        batch1(0).veType.cEnumValue, batch1(0).numItems, batch1(0).buffers(0).limit, batch1(0).buffers(1).limit,
        batch2(0).veType.cEnumValue, batch2(0).numItems, batch2(0).buffers(0).limit, batch2(0).buffers(1).limit,
        batch3(0).veType.cEnumValue, batch3(0).numItems, batch3(0).buffers(0).limit, batch3(0).buffers(1).limit,
        // Column headers for the second column of each batch
        batch1(1).veType.cEnumValue, batch1(1).numItems, batch1(1).buffers(0).limit, batch1(1).buffers(1).limit,
        batch2(1).veType.cEnumValue, batch2(1).numItems, batch2(1).buffers(0).limit, batch2(1).buffers(1).limit,
        batch3(1).veType.cEnumValue, batch3(1).numItems, batch3(1).buffers(0).limit, batch3(1).buffers(1).limit,
        // Column headers for the third column of each batch
        batch1(2).veType.cEnumValue, batch1(2).numItems, batch1(2).buffers(0).limit, batch1(2).buffers(1).limit,
        batch2(2).veType.cEnumValue, batch2(2).numItems, batch2(2).buffers(0).limit, batch2(2).buffers(1).limit,
        batch3(2).veType.cEnumValue, batch3(2).numItems, batch3(2).buffers(0).limit, batch3(2).buffers(1).limit,
        // Column headers for the fourth column of each batch
        batch1(3).veType.cEnumValue, batch1(3).numItems, batch1(3).buffers(0).limit, batch1(3).buffers(1).limit, batch1(3).buffers(2).limit, batch1(3).buffers(3).limit,
        batch2(3).veType.cEnumValue, batch2(3).numItems, batch2(3).buffers(0).limit, batch2(3).buffers(1).limit, batch2(3).buffers(2).limit, batch2(3).buffers(3).limit,
        batch3(3).veType.cEnumValue, batch3(3).numItems, batch3(3).buffers(0).limit, batch3(3).buffers(1).limit, batch3(3).buffers(2).limit, batch3(3).buffers(3).limit,
      ))
    }

    "correctly have vector-aligned (8-byte aligned) data offsets" in {
      val descriptor = sampleDescriptor
      val offsets = descriptor.dataOffsets

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
      descriptor.buffer.slice(offsets(0), offsets(1) - offsets(0)).as[IntPointer].toArray.map(_.toShort) should be (Array[Short](1, 2))
      descriptor.buffer.slice(offsets(2), offsets(3) - offsets(2)).as[IntPointer].toArray.map(_.toShort) should be (Array[Short](3, 4, 5, 0))
      descriptor.buffer.slice(offsets(4), offsets(5) - offsets(4)).as[IntPointer].toArray.map(_.toShort) should be (Array[Short](6, 7, 8, 9))
      // Column A validity buffers should be correctly written
      descriptor.buffer.slice(offsets(1), offsets(2) - offsets(1)).toArray should be (FixedBitSet.ones(2).toByteArray)
      descriptor.buffer.slice(offsets(3), offsets(4) - offsets(3)).toArray should be (FixedBitSet.ones(3).toByteArray)
      descriptor.buffer.slice(offsets(5), offsets(6) - offsets(5)).toArray should be (FixedBitSet.ones(4).toByteArray)

      // Column B data should be correctly written
      descriptor.buffer.slice(offsets(6), offsets(7) - offsets(6)).as[FloatPointer].toArray should be (Array[Float](10, 20))
      descriptor.buffer.slice(offsets(8), offsets(9) - offsets(8)).as[FloatPointer].toArray should be (Array[Float](30, 40, 50, 0))
      descriptor.buffer.slice(offsets(10), offsets(11) - offsets(10)).as[FloatPointer].toArray should be (Array[Float](60, 70, 80, 90))
      // Column B validity buffers should be correctly written
      descriptor.buffer.slice(offsets(7), offsets(8) - offsets(7)).toArray should be (FixedBitSet.ones(2).toByteArray)
      descriptor.buffer.slice(offsets(9), offsets(10) - offsets(9)).toArray should be (FixedBitSet.ones(3).toByteArray)
      descriptor.buffer.slice(offsets(11), offsets(12) - offsets(11)).toArray should be (FixedBitSet.ones(4).toByteArray)

      // Column C data should be correctly written
      descriptor.buffer.slice(offsets(12), offsets(13) - offsets(12)).as[DoublePointer].toArray should be (Array[Double](100, 200))
      descriptor.buffer.slice(offsets(14), offsets(15) - offsets(14)).as[DoublePointer].toArray should be (Array[Double](300, 400, 500))
      descriptor.buffer.slice(offsets(16), offsets(17) - offsets(16)).as[DoublePointer].toArray should be (Array[Double](600, 700, 800, 900))
      // Column C validity buffers should be correctly written
      descriptor.buffer.slice(offsets(13), offsets(14) - offsets(13)).toArray should be (FixedBitSet.ones(2).toByteArray)
      descriptor.buffer.slice(offsets(15), offsets(16) - offsets(15)).toArray should be (FixedBitSet.ones(3).toByteArray)
      descriptor.buffer.slice(offsets(17), offsets(18) - offsets(17)).toArray should be (FixedBitSet.ones(4).toByteArray)
    }

    "correctly write varchar columns into the data buffers" in {
      val descriptor = sampleDescriptor
      val buffer = descriptor.buffer
      val offsets = descriptor.dataOffsets

      // Column D data should be correctly written
      descriptor.buffer.slice(offsets(18), offsets(19) - offsets(18)).toArray should be (Array("apple", "banana").map(_.getBytes("UTF-32LE")).flatten ++ Array[Byte](0, 0, 0, 0))
      descriptor.buffer.slice(offsets(22), offsets(23) - offsets(22)).toArray should be (Array("carrot", "durian", "eggplant").map(_.getBytes("UTF-32LE")).flatten)
      descriptor.buffer.slice(offsets(26), offsets(27) - offsets(26)).toArray should be (Array("fig", "grape", "hawthorn", "ichigo").map(_.getBytes("UTF-32LE")).flatten)

      // Column D offsets should be correctly written
      descriptor.buffer.slice(offsets(19), offsets(20) - offsets(19)).as[IntPointer].toArray should be (Array(0, 5))
      descriptor.buffer.slice(offsets(23), offsets(24) - offsets(23)).as[IntPointer].toArray should be (Array(0, 6, 12, 0))
      descriptor.buffer.slice(offsets(27), offsets(28) - offsets(27)).as[IntPointer].toArray should be (Array(0, 3, 8, 16))

      // Column D lens should be correctly written
      descriptor.buffer.slice(offsets(20), offsets(21) - offsets(20)).as[IntPointer].toArray should be (Array("apple", "banana").map(_.size))
      descriptor.buffer.slice(offsets(24), offsets(25) - offsets(24)).as[IntPointer].toArray should be (Array("carrot", "durian", "eggplant").map(_.size) ++ Array(0))
      descriptor.buffer.slice(offsets(28), offsets(29) - offsets(28)).as[IntPointer].toArray should be (Array("fig", "grape", "hawthorn", "ichigo").map(_.size))

      // Column D validity buffers should be correctly written
      descriptor.buffer.slice(offsets(21), offsets(22) - offsets(21)).toArray should be (FixedBitSet.ones(2).toByteArray)
      descriptor.buffer.slice(offsets(25), offsets(26) - offsets(25)).toArray should be (FixedBitSet.ones(3).toByteArray)
      descriptor.buffer.slice(offsets(29), offsets(30) - offsets(29)).toArray should be (FixedBitSet.ones(4).toByteArray)
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

      val batch = descriptor.resultToColBatch
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

    "correctly unpack multiple batches of mixed vector types" in {
      val descriptor = sampleDescriptor
      val batch = engine.executeTransfer(descriptor)

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

      col1.toBytePointerColVector2.toArray[Short] should be (Array[Short](1, 2, 3, 4, 5, 6, 7, 8, 9))
      col2.toBytePointerColVector2.toArray[Float] should be (Array[Float](10, 20, 30, 40, 50, 60, 70, 80, 90))
      col3.toBytePointerColVector2.toArray[Double] should be (Array[Double](100, 200, 300, 400, 500, 600, 700, 800, 900))
      col4.toBytePointerColVector2.toArray[String] should be (Array[String]("apple", "banana", "carrot", "durian", "eggplant", "fig", "grape", "hawthorn", "ichigo"))
    }
  }
}
