package com.nec.colvector

import com.nec.cache.TransferDescriptor
import com.nec.colvector.ArrayTConversions._
import com.nec.colvector.SeqOptTConversions._
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.util.FixedBitSet
import com.nec.ve.WithVeProcess
import com.nec.vectorengine.LibCyclone
import org.bytedeco.javacpp.LongPointer
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import java.nio.file.Paths

@VectorEngineTest
final class PackedTransferSpec extends AnyWordSpec with WithVeProcess {
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

      val libRef = veProcess.loadLibrary(LibCyclone.SoPath)
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

      val libRef = veProcess.loadLibrary(LibCyclone.SoPath)
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

    "correctly transfer a batch of Seq[Option[Int]] to the VE and back without loss of data fidelity [1]" in {
      // Batch A
      val a1 = Seq(None, Some(4436), None, None, Some(9586), Some(2142))

      // Batch B
      val b1 = Seq(None, None, None, Some(8051))

      // Batch C
      val c1 = Seq(Some(7319), None, None, Some(4859), Some(524))

      // Create batch of batches
      val descriptor = TransferDescriptor(Seq(
        Seq(a1.toBytePointerColVector("_")),
        Seq(b1.toBytePointerColVector("_")),
        Seq(c1.toBytePointerColVector("_"))
      ))


      val lib = veProcess.loadLibrary(LibCyclone.SoPath)
      val batch = veProcess.executeTransfer(lib, descriptor)


      batch.columns.size should be (1)
      batch.columns(0).toBytePointerColVector.toSeqOpt[Int] should be (a1 ++ b1 ++ c1)
    }

    "correctly transfer a batch of Seq[Option[Int]] to the VE and back without loss of data fidelity [2]" ignore {
      // Batch A
      val a1 = Seq(None, Some(4436), None, None, Some(9586), Some(2142), None, None, None, Some(2149), Some(4297), None, None, Some(3278), Some(6668), None)

      // Batch B
      val b1 = Seq(None, None, None, Some(8051), None, Some(1383), None, None, Some(2256), Some(5785), None, None, None, None, None, Some(4693), None, Some(1849), Some(3790), Some(8995), None, Some(6961), Some(7132), None, None, None, None, Some(6968), None, None, Some(3763), None, Some(3558), None, None, Some(2011), None, None, None, Some(3273), None, None, Some(9428), None, None, Some(6408), Some(7940), None, Some(9521), None, None, Some(5832), None, None, Some(5817), Some(5949))

      // Batch C
      val c1 = Seq(Some(7319), None, None, Some(4859), Some(524), Some(406), None, None, Some(1154), None, None, Some(1650), Some(8040), None, None, None, None, None, None, None, None, None, Some(1146), None, Some(7268), Some(8197), None, None, None, None, Some(81), Some(2053), Some(6571), Some(4600), None, Some(3699), None, Some(8404), None, None, Some(8401), None, None, Some(6234), Some(6281), Some(7367), None, Some(4688), Some(7490), None, Some(5412), None, None, Some(871), None, Some(9086), None, Some(5362), Some(6516))

      // Create BytePointerColVectors
      val a1v = a1.toBytePointerColVector("_")
      val b1v = b1.toBytePointerColVector("_")
      val c1v = c1.toBytePointerColVector("_")

      // Create batch of batches
      val descriptor = TransferDescriptor(Seq(
        Seq(a1v),
        Seq(b1v),
        Seq(c1v)
      ))

      val lib = veProcess.loadLibrary(LibCyclone.SoPath)
      val batch = veProcess.executeTransfer(lib, descriptor)

      batch.columns.size should be (1)
      val output = batch.columns(0).toBytePointerColVector

      // If we extract as Array[Int], the values match with the input
      output.toArray[Int].toSeq should be ((a1 ++ b1 ++ c1).map(_.getOrElse(0)))

      // The lengths match as well
      output.toArray[Int].size should be (a1.size + b1.size + c1.size)

      val a1bits = FixedBitSet.from(a1v.buffers(1))
      val b1bits = FixedBitSet.from(b1v.buffers(1))
      val c1bits = FixedBitSet.from(c1v.buffers(1))
      val outbits = FixedBitSet.from(output.buffers(1)).toSeq

      // But the output bitset does not match the merge of the 3 input bitsets,,,
      println(a1bits.toSeq.take(a1.size))
      println(b1bits.toSeq.take(b1.size))
      println(c1bits.toSeq.take(c1.size))
      println(outbits.toSeq)
    }
  }
}
