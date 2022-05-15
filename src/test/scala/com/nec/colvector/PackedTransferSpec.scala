package com.nec.colvector

import com.nec.cache.TransferDescriptor
import com.nec.colvector.ArrayTConversions._
import com.nec.colvector.SeqOptTConversions._
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.util.CallContextOps._
import com.nec.ve.WithVeProcess
import com.nec.util.FixedBitSet
import com.nec.vectorengine.WithVeProcess
import com.nec.vectorengine.LibCyclone
import scala.reflect.runtime.universe._
import scala.util.Random
import org.bytedeco.javacpp.LongPointer
import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers._
import org.scalatest.prop.TableDrivenPropertyChecks.whenever
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks.forAll
import scala.reflect.ClassTag

@VectorEngineTest
final class PackedTransferSpec extends AnyWordSpec with WithVeProcess {
  private lazy val libRef = veProcess.loadLibrary(LibCyclone.SoPath)

  "C++ handle_transfer()" should {
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

      val batch = veProcess.executeTransfer(libRef, descriptor)

      batch.columns.size should be (1)
      batch.columns(0).toBytePointerColVector.toSeqOpt[Int] should be (a1 ++ b1 ++ c1)
    }

    "correctly transfer a batch of Seq[Option[Int]] to the VE and back without loss of data fidelity [2]" in {
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

      val batch = veProcess.executeTransfer(libRef, descriptor)

      batch.columns.size should be (1)
      val output = batch.columns(0).toBytePointerColVector

      // If we extract as Array[Int], the values match with the input
      output.toArray[Int].toSeq should be ((a1 ++ b1 ++ c1).map(_.getOrElse(0)))

      // The lengths match as well
      output.toArray[Int].size should be (a1.size + b1.size + c1.size)

      val a1bits = FixedBitSet.from(a1v.buffers(1))
      val b1bits = FixedBitSet.from(b1v.buffers(1))
      val c1bits = FixedBitSet.from(c1v.buffers(1))
      val outbits = FixedBitSet.from(output.buffers(1))

      outbits.toSeq.take(a1.size + b1.size + c1.size) should equal(a1bits.toSeq.take(a1.size) ++ b1bits.toSeq.take(b1.size) ++ c1bits.toSeq.take(c1.size))
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

    def generatedColumns[T: ClassTag]: Gen[Seq[Seq[Option[T]]]] = {
      Gen.zip(Gen.choose[Int](1, 37), Gen.choose[Int](1, 1024))
        .map { case (ncolumns, length) =>
          (0 until ncolumns).map{ i => InputSamples.seqOpt[T](length) }.toSeq
        }
    }

    def generatedBatches[T: ClassTag](maxLength: Int = 512): Gen[Seq[Seq[Seq[Option[T]]]]] = {
      Gen.zip(Gen.choose[Int](1, 10), Gen.choose[Int](1, 10), Gen.choose[Int](1, maxLength))
        .map { case (nbatches, ncolumns, length) =>
          (0 until nbatches).map { batch =>
            (0 until ncolumns).map { col =>
              InputSamples.seqOpt[T](length)
            }.toSeq
          }.toSeq
        }
    }

    def generatedMixedBatches(typetags: TypeTag[_]*): Gen[Seq[Seq[BytePointerColVector]]] = {
      Gen.zip(Gen.choose[Int](1, 1024), Gen.choose[Int](1, 10)).map { case (nbatches, size) =>
        (0 until nbatches).toSeq.map { batch =>
          typetags.toSeq.map { tag =>
            if (tag.tpe =:= typeOf[Short]) {
              InputSamples.seqOpt[Short](size).toBytePointerColVector("_")

            } else if (tag.tpe =:= typeOf[Int]) {
              InputSamples.seqOpt[Int](size).toBytePointerColVector("_")

            } else if (tag.tpe =:= typeOf[Long]) {
              InputSamples.seqOpt[Long](size).toBytePointerColVector("_")

            } else if (tag.tpe =:= typeOf[Float]) {
              InputSamples.seqOpt[Float](size).toBytePointerColVector("_")

            } else if (tag.tpe =:= typeOf[Double]) {
              InputSamples.seqOpt[Double](size).toBytePointerColVector("_")

            } else {
              InputSamples.seqOpt[String](size).toBytePointerColVector("_")
            }
          }
        }
      }
    }

    def generatedAnyMixBatches: Gen[Seq[Seq[BytePointerColVector]]] = {
      Gen.choose[Int](1, 32).flatMap { ncolumns =>
        val typetags = Stream.continually(Seq(typeTag[Int], typeTag[Short], typeTag[Long], typeTag[Float], typeTag[Double], typeTag[String])).flatten
          .take(ncolumns)

        generatedMixedBatches(Random.shuffle(typetags).toArray: _*)
      }
    }

    "satisfy the property: All single scalar batches unpack correctly" in {
      forAll (generatedColumns[Int]) { cols =>
        whenever (cols.nonEmpty && cols.forall(_.nonEmpty)) {
          val descriptor = new TransferDescriptor(Seq(cols.map(_.toBytePointerColVector("_"))))
          val batch = veProcess.executeTransfer(libRef, descriptor)

          batch.columns.size should be (cols.length)
          batch.columns.zipWithIndex.foreach{ case (col, i) =>
            col.toBytePointerColVector.toBytes should equal (cols(i).toBytePointerColVector("_").toBytes)
          }

          batch.free()
        }
      }
    }

    "satisfy the property: All single varchar batches unpack correctly" in {
      forAll (generatedColumns[String]) { cols =>
        whenever(cols.nonEmpty && cols.forall(_.nonEmpty)) {
          val descriptor = new TransferDescriptor(List(cols.map(_.toBytePointerColVector("_"))))
          val batch = veProcess.executeTransfer(libRef, descriptor)

          batch.columns.size should be(cols.length)
          batch.columns.zipWithIndex.foreach { case (col, i) =>
            col.toBytePointerColVector.toBytes should equal(cols(i).toBytePointerColVector("_").toBytes)
          }

          batch.free()
        }
      }
    }

    "satisfy the property: All sets of scalar batches unpack correctly" in {
      forAll (generatedBatches[Int](2048)) { batches =>
        whenever (batches.nonEmpty && batches.forall{b => b.nonEmpty && b.forall(_.nonEmpty)} && batches.forall(_.size == batches.head.size)) {
          val descriptor = new TransferDescriptor(batches.map(_.map(_.toBytePointerColVector("_"))))
          val batch = veProcess.executeTransfer(libRef, descriptor)
          val mergedCols = batches.transpose.map(_.flatten)

          batch.columns.size should be (batches.head.length)
          batch.numRows should be (mergedCols.head.size)
          batch.columns.zipWithIndex.foreach { case (col, i) =>
            col.toBytePointerColVector.toBytes should equal (mergedCols(i).toBytePointerColVector("_").toBytes)
          }

          batch.free()
        }
      }
    }

    "satisfy the property: All sets of varchar batches unpack correctly" in {
      forAll (generatedBatches[String](2048)) { batches =>
        whenever(batches.nonEmpty && batches.forall{b => b.nonEmpty && b.forall(_.nonEmpty)} && batches.forall(_.size == batches.head.size)){
          val descriptor = new TransferDescriptor(batches.map(_.map(_.toBytePointerColVector("_"))))
          val batch = veProcess.executeTransfer(libRef, descriptor)
          val mergedCols = batches.transpose.map(_.flatten)

          batch.columns.size should be(batches.head.length)
          batch.numRows should be(mergedCols.head.size)
          batch.columns.zipWithIndex.foreach{ case (col, i) =>
            col.toBytePointerColVector.toBytes should equal(mergedCols(i).toBytePointerColVector("_").toBytes)
          }

          batch.free()
        }
      }
    }

    "satisfy the property: All sets of mixed batches unpack correctly" in {
      forAll (generatedAnyMixBatches) { batches =>
        whenever (batches.nonEmpty && batches.forall { b => b.nonEmpty && b.forall(_.numItems > 0) } && batches.forall(_.size == batches.head.size)) {
          val descriptor = new TransferDescriptor(batches)
          val transposed = batches.transpose

          val batch = veProcess.executeTransfer(libRef, descriptor)
          batch.columns.size should be(batches.head.length)
          batch.numRows should be (transposed.head.map(_.numItems).sum)

          (batch.columns, transposed).zipped.foreach { case (column, vecs) =>
            column.toBytePointerColVector.toSeqOptAny should be (vecs.map(_.toSeqOptAny).flatten)
          }

          batch.free()
        }
      }
    }
  }
}
