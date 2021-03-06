package io.sparkcyclone.data.transfer

import io.sparkcyclone.data.InputSamples
import io.sparkcyclone.data.conversion.SeqOptTConversions._
import io.sparkcyclone.annotations.VectorEngineTest
import io.sparkcyclone.util.CallContextOps._
import io.sparkcyclone.util.FixedBitSet
import io.sparkcyclone.util.PointerOps._
import io.sparkcyclone.vectorengine.WithVeProcess
import scala.util.Random
import org.bytedeco.javacpp._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

@VectorEngineTest
final class UcvTransferDescriptorUnitSpec extends AnyWordSpec with WithVeProcess {
  "UcvTransferDescriptor" should {
    "function seamlessly as a TransferDescriptor, but with an externally-provided transfer buffer" in {
      val size = Random.nextInt(100) + 10
      val input1 = InputSamples.seqOpt[Int](size)
      val input2 = InputSamples.seqOpt[Double](size)
      val input3 = InputSamples.seqOpt[String](size)

      val columns = Seq(
        input1.toBytePointerColVector("_"),
        input2.toBytePointerColVector("_"),
        input3.toBytePointerColVector("_")
      )

      // Create BpcvTransferDescriptor
      val descriptor1 = BpcvTransferDescriptor(Seq(columns))

      // Generate the transfer buffer and move to UcvTransferDescriptor
      val descriptor2 = UcvTransferDescriptor(columns.map(_.toUnitColVector), descriptor1.buffer)

      // Copy to VE and get back VeColBatch
      val batch = engine.executeTransfer(descriptor2)

      batch.columns.size should be (3)
      batch.columns(0).toBytePointerColVector.toSeqOpt[Int] should be (input1)
      batch.columns(1).toBytePointerColVector.toSeqOpt[Double] should be (input2)
      batch.columns(2).toBytePointerColVector.toSeqOpt[String] should be (input3)
    }
  }
}
