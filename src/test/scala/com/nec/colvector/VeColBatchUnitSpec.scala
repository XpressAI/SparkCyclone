package com.nec.colvector

import com.nec.colvector.ArrayTConversions._
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.vectorengine.WithVeProcess
import com.nec.util.CallContextOps._
import java.io._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

@VectorEngineTest
final class VeColBatchUnitSpec extends AnyWordSpec with WithVeProcess {
  "VeColBatch" should {
    "correctly serialize and deserialize a batch of 2 columns through Array[Byte]" in {
      val array1 = Array[Double](1, 2, 3)
      val array2 = Array[Double](-1, -2, -3)

      val batch1 = VeColBatch(Seq(array1, array2).map(_.toBytePointerColVector("_").toVeColVector))
      val batch2 = VeColBatch.fromBytes(batch1.toBytes)

      val output = batch2.columns.map(_.toBytePointerColVector.toArray[Double].toSeq)
      output should be (Seq(array1, array2).map(_.toSeq))
    }

    "correctly serialize and deserialize a batch of 2 columns through java.io.OutputStream and java.io.InputStream" in {
      val array1 = Array[Double](1, 2, 3)
      val array2 = Array[Double](-1, -2, -3)
      val batch1 = VeColBatch(Seq(array1, array2).map(_.toBytePointerColVector("_").toVeColVector))

      val bostream = new ByteArrayOutputStream
      val ostream = new DataOutputStream(bostream)
      batch1.toStream(ostream)

      val bistream = new ByteArrayInputStream(bostream.toByteArray)
      val istream = new DataInputStream(bistream)
      val batch2 = VeColBatch.fromStream(istream)

      val output = batch2.columns.map(_.toBytePointerColVector.toArray[Double].toSeq)
      output should be (Seq(array1, array2).map(_.toSeq))
    }
  }
}
