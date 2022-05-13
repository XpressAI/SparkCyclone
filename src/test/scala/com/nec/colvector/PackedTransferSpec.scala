package com.nec.colvector

import com.nec.cache.TransferDescriptor
import com.nec.colvector.ArrayTConversions._
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.ve.WithVeProcess
import org.bytedeco.javacpp.LongPointer
import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers._
import org.scalatest.prop.TableDrivenPropertyChecks.whenever
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks.forAll

import java.nio.file.Paths
import scala.reflect.ClassTag


@VectorEngineTest
final class PackedTransferSpec extends AnyWordSpec with WithVeProcess {
  val libraryPath = Paths.get("target/scala-2.12/classes/cycloneve/libcyclone.so")
  lazy val libRef = veProcess.loadLibrary(libraryPath)

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

    "correctly unpack a batch of two columns of a single item of an empty string" in {
      val input = List(Array(""), Array(""))
      val inputBatch = List(input.map(_.toBytePointerColVector("_")))
      val descriptor = new TransferDescriptor(inputBatch)

      val batch = veProcess.executeTransfer(libRef, descriptor)

      batch.numRows should be(1)
      batch.columns.size should be(2)

      batch.columns.zip(input).foreach{ case (bc, inCol)  =>
        bc.toBytePointerColVector.toBytes should equal(inCol.toBytePointerColVector("_").toBytes)
      }
    }

    "correctly unpack a batch of two columns of a 3-items of an non-empty string" in {
      val input = List(Array("a", "c", "e"), Array("b", "d", "f"))
      val inputBatch = List(input.map(_.toBytePointerColVector("_")))
      val descriptor = new TransferDescriptor(inputBatch)

      val batch = veProcess.executeTransfer(libRef, descriptor)

      batch.numRows should be(3)
      batch.columns.size should be(2)

      batch.columns.zip(input).foreach{ case (bc, inCol)  =>
        bc.toBytePointerColVector.toBytes should equal(inCol.toBytePointerColVector("_").toBytes)
      }
    }

    def generatedColumn[T: ClassTag] = Gen.choose[Int](1, 512).map(InputSamples.seq[T](_))
    def generatedColumns[T: ClassTag] = Gen.zip(Gen.choose[Int](1, 25), Gen.choose[Int](1, 25)).map{ case (colCount, colLength) =>
      (0 until colCount).map{ i => InputSamples.seq[T](colLength) }.toList
    }
    def generatedBatches[T: ClassTag](maxLength: Int = 512) = Gen.zip(Gen.choose[Int](1, 10), Gen.choose[Int](1, 10), Gen.choose[Int](1, maxLength)).map{ case(batchCount, colCount, colLength) =>
      (0 until batchCount).map{batch =>
        (0 until colCount).map{col =>
          InputSamples.seq[T](colLength)
        }.toList
      }.toList
    }

    def generatedMixedBatches(klasses: Class[_]*) = {
      Gen.zip(Gen.choose[Int](1, 10), Gen.choose[Int](1, 10)).map{ case (batchCount, colLength) =>
        (0 until batchCount).map{batch =>
          klasses.map{ klass =>
            InputSamples.seq(colLength)(ClassTag(klass))
          }.toList
        }.toList
      }
    }

    def generatedAnyMixBatches = {
      Gen.choose[Int](1, 25).flatMap{ colCount =>
        Gen.pick(colCount,
          Stream.continually(Seq(classOf[Int], classOf[Short], classOf[Long], classOf[Float], classOf[Double], classOf[String])).flatten
        ).flatMap{ colClasses =>
          generatedMixedBatches(colClasses.toArray:_*)
        }
      }
    }

    "satisfy the property: All single scalar batches unpack correctly" in {
      forAll(generatedColumns[Int]){ cols =>
        whenever(cols.nonEmpty && cols.forall(_.nonEmpty)){
          val descriptor = new TransferDescriptor(List(cols.map(_.toArray.toBytePointerColVector("_"))))

          val batch = veProcess.executeTransfer(libRef, descriptor)

          batch.columns.size should be(cols.length)

          batch.columns.zipWithIndex.foreach{ case (col, i) =>
            col.toBytePointerColVector.toArray[Int] should equal(cols(i))
          }
          batch.free()
        }
      }
    }

    "satisfy the property: All single varchar batches unpack correctly" in {
      forAll(generatedColumns[String]){ cols =>
        whenever(cols.nonEmpty && cols.forall(_.nonEmpty)) {
          val descriptor = new TransferDescriptor(List(cols.map(_.toArray.toBytePointerColVector("_"))))

          val batch = veProcess.executeTransfer(libRef, descriptor)

          batch.columns.size should be(cols.length)

          batch.columns.zipWithIndex.foreach { case (col, i) =>
            col.toBytePointerColVector.toArray[String] should equal(cols(i))
          }
          batch.free()
        }
      }
    }

    "satisfy the property: All sets of scalar batches unpack correctly" in {
      forAll(generatedBatches[Int](512)){ batches =>
        whenever(batches.nonEmpty && batches.forall{b => b.nonEmpty && b.forall(_.nonEmpty)} && batches.forall(_.size == batches.head.size)){
          val descriptor = new TransferDescriptor(batches.map(_.map(_.toArray.toBytePointerColVector("_"))))

          val mergedCols = batches.transpose.map(_.flatten)

          val batch = veProcess.executeTransfer(libRef, descriptor)

          batch.columns.size should be(batches.head.length)
          batch.numRows should be(mergedCols.head.size)

          batch.columns.zipWithIndex.foreach{ case (col, i) =>
            col.toBytePointerColVector.toArray[Int] should equal(mergedCols(i))
          }
          batch.free()
        }
      }
    }

    "satisfy the property: All sets of varchar batches unpack correctly" in {
      forAll(generatedBatches[String](50)){ (batches) =>
        whenever(batches.nonEmpty && batches.forall{b => b.nonEmpty && b.forall(_.nonEmpty)} && batches.forall(_.size == batches.head.size)){
          val descriptor = new TransferDescriptor(batches.map(_.map(_.toArray.toBytePointerColVector("_"))))

          val mergedCols = batches.transpose.map(_.flatten)

          //println(s"batches: ${batches}")
          //descriptor.printBuffer()

          val batch = veProcess.executeTransfer(libRef, descriptor)

          batch.columns.size should be(batches.head.length)
          batch.numRows should be(mergedCols.head.size)

          batch.columns.zipWithIndex.foreach{ case (col, i) =>
            col.toBytePointerColVector.toArray[String] should equal(mergedCols(i))
          }
          batch.free()
        }
      }
    }

    "satisfy the property: All sets of mixed batches unpack correctly" ignore {
      forAll(generatedAnyMixBatches) { batches =>
        whenever(batches.nonEmpty && batches.forall { b => b.nonEmpty && b.forall(_.nonEmpty) } && batches.forall(_.size == batches.head.size)) {
          println("before descriptor")
          val descriptor = new TransferDescriptor(batches.map(_.map(_.toArray.toBytePointerColVector("_"))))

          println("before merge")
          val mergedCols = batches.transpose.map(_.flatten)

          println("before transfer")
          val batch = veProcess.executeTransfer(libRef, descriptor)

          println("before sanity")
          batch.columns.size should be(batches.head.length)
          batch.numRows should be(mergedCols.head.size)

          println("before actual")
          batch.columns.zipWithIndex.foreach { case (col, i) =>
            println(s"printing $i")
            val tag: ClassTag[_] = ClassTag(col.veType.scalaType)
            col.toBytePointerColVector.toArray(tag) should equal(mergedCols(i))
          }
          batch.free()
        }
      }
    }
  }
}
