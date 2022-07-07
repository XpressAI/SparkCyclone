package io.sparkcyclone.data.serialization

import io.sparkcyclone.colvector.SeqOptTConversions._
import io.sparkcyclone.colvector.{InputSamples, VeColBatch}
import io.sparkcyclone.annotations.VectorEngineTest
import io.sparkcyclone.util.CallContextOps._
import io.sparkcyclone.data.serialization.DualBatchOrBytes.ColBatchWrapper
import io.sparkcyclone.vectorengine.WithVeProcess
import scala.util.Random
import java.io._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

@VectorEngineTest
final class VeSerializerSpec extends AnyWordSpec with WithVeProcess {
  "VeSerializer" should {
    s"correctly serialize and deserialize ${classOf[VeColBatch]}'s" in {
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
      val ostream = new VeSerializationStream(bostream)
      ostream.writeObject(ColBatchWrapper(batch1))
      ostream.flush
      ostream.close

      val bistream = new ByteArrayInputStream(bostream.toByteArray)
      val istream = new VeDeserializationStream(bistream)
      val bytes = istream
        .readObject[DualBatchOrBytes]()
        .fold(x => x, _ => sys.error(s"Got col batch, expected bytes"))

      val batch2 = VeColBatch.fromStream(new DataInputStream(new ByteArrayInputStream(bytes.bytes)))

      batch2.columns.size should be (3)
      batch2.columns(0).toBytePointerColVector.toSeqOpt[Int] should be (input1)
      batch2.columns(1).toBytePointerColVector.toSeqOpt[Double] should be (input2)
      batch2.columns(2).toBytePointerColVector.toSeqOpt[String] should be (input3)
    }
  }
}
