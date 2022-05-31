package com.nec.ve.serializer

import com.nec.colvector.ArrayTConversions._
import com.nec.colvector.VeColBatch
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.ve.serializer.DualBatchOrBytes.ColBatchWrapper
import com.nec.ve.VeKernelInfra
import com.nec.vectorengine.WithVeProcess
import java.io._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

@VectorEngineTest
final class VeSerializerSpec extends AnyWordSpec with WithVeProcess with VeKernelInfra {
  import com.nec.util.CallContextOps._

  "VeSerializer" should {
    "check serializer fully" in {
      val array1 = Array[Double](1, 2, 3)
      val array2 = Array[Double](-1, -2, -3)
      val batch1 = VeColBatch(Seq(array1, array2).map(_.toBytePointerColVector("_").toVeColVector))

      val bostream = new ByteArrayOutputStream()
      val ostream = new VeSerializationStream(bostream)
      ostream.writeObject(ColBatchWrapper(batch1))
      ostream.flush
      ostream.close

      val bistream = new ByteArrayInputStream(bostream.toByteArray)
      val istream = new VeDeserializationStream(bistream)
      val bytesOnly = istream
        .readObject[DualBatchOrBytes]()
        .fold(bytesOnly => bytesOnly, _ => sys.error(s"Got col batch, expected bytes"))

      val batch2 = VeColBatch.fromStream(new DataInputStream(new ByteArrayInputStream(bytesOnly.bytes)))
      val output = batch2.columns.map(_.toBytePointerColVector.toArray[Double].toSeq)
      output should be(Seq(array1, array2).map(_.toSeq))
    }
  }
}
