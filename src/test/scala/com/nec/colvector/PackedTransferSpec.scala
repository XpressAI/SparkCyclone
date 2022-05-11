package com.nec.colvector

import com.nec.cache.TransferDescriptor
import com.nec.colvector.ArrayTConversions._
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.ve.WithVeProcess
import org.bytedeco.javacpp.LongPointer
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.Paths


@VectorEngineTest
final class PackedTransferSpec extends AnyWordSpec with WithVeProcess {
  val libraryPath = Paths.get("target/scala-2.12/classes/cycloneve/libcyclone.so")

  private def batch1() = Seq(
    Array[Double](1, 2, 3).toBytePointerColVector("col1_b1"),
    Array[String]("a", "b", "c").toBytePointerColVector("col2_b1")
  )
  private def batch2() = Seq(
    Array[Double](4, 5, 6).toBytePointerColVector("col1_b2"),
    Array[String]("d", "e", "f").toBytePointerColVector("col2_b2")
  )
  private def batch3() = Seq(
    Array[Double](7, 8).toBytePointerColVector("col1_b3"),
    Array[String]("g", "h").toBytePointerColVector("col2_b3")
  )

  private def transferDescriptor() = {
    val descriptor = new TransferDescriptor.Builder()
      .newBatch().addColumns(batch1())
      .newBatch().addColumns(batch2())
      .newBatch().addColumns(batch3())
      .build()

    descriptor
  }

  "TransferDescriptor" should {
    "correctly create the transfer buffer header" in {
      val descriptor = transferDescriptor()
      val buffer = descriptor.buffer
      val longBuffer = new LongPointer(buffer)
      val header = new Array[Long](3)
      longBuffer.get(header)

      val expectedBatchCount = 3
      val expectedColumnCount = 2
      val expectedHeaderSize = (3 + (expectedBatchCount * (4  + 6))) * 8
      val expectedHeader = Array[Long](expectedHeaderSize, expectedBatchCount, expectedColumnCount)

      header should be (expectedHeader)
    }

    "correctly read the output buffer" in {
      val descriptor = transferDescriptor()
      val outputBuffer = descriptor.outputBuffer

      val expectedOutputBufferSize = (5 + 3) * 8
      outputBuffer.limit() should be (expectedOutputBufferSize)

      // put up some imaginary values
      val longOD = new LongPointer(outputBuffer)
      // Col 1
        .put(0, 1)
        .put(1, 2)
        .put(2, 3)
      // Col 2
        .put(3, 4)
        .put(4, 5)
        .put(5, 6)
        .put(6, 7)
        .put(7, 8)

      val batch = descriptor.outputBufferToColBatch()
      batch.numRows should be (8)
      batch.columns.size should be (2)

      val col1 = batch.columns(0)
      val col2 = batch.columns(1)

      col1.numItems should be (8)
      col2.numItems should be (8)

      col1.container should be (1)
      col1.buffers should be (Seq(2, 3))

      col2.container should be (4)
      col2.buffers should be (Seq(5, 6, 7, 8))
    }
  }

  "handle_transfer" should {
    import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

    "correctly unpack a single batch of mixed vector types" in {
      val cols = batch1()
      val descriptor = new TransferDescriptor.Builder()
        .newBatch().addColumns(cols)
        .build()

      val libRef = veProcess.loadLibrary(libraryPath)
      val batch = veProcess.executeTransfer(libRef, descriptor)

      batch.numRows should be (3)
      batch.columns.size should be (2)

      val col1 = batch.columns(0)
      val col2 = batch.columns(1)

      col1.numItems should be(3)
      col2.numItems should be(3)

      col1.toBytePointerColVector.toBytes should equal (cols(0).toBytes)
      col2.toBytePointerColVector.toBytes should equal (cols(1).toBytes)
    }

    "correctly unpack multiple batches of mixed vector types" in {
      val descriptor = transferDescriptor()
      println("Transfer Buffer = ")
      val buffer = Array.ofDim[Byte](descriptor.buffer.limit().toInt)
      descriptor.buffer.get(buffer)
      println(buffer.mkString("Array(", ", ", ")"))

      val libRef = veProcess.loadLibrary(libraryPath)
      val batch = veProcess.executeTransfer(libRef, descriptor)

      batch.numRows should be (8)
      batch.columns.size should be (2)

      val col1 = batch.columns(0)
      val col2 = batch.columns(1)

      col1.numItems should be (8)
      col2.numItems should be (8)

      val col1_merged = Array[Double](1,2,3,4,5,6,7,8).toBytePointerColVector("expected_col1").toBytes
      val col2_merged = Array[String]("a", "b", "c", "d", "e", "f", "g", "h").toBytePointerColVector("expected_col2").toBytes


      col1.toBytePointerColVector.toBytes should equal(col1_merged)
      col2.toBytePointerColVector.toBytes should equal(col2_merged)
    }
  }
}
