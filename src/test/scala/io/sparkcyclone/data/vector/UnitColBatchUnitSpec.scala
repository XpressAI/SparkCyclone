package io.sparkcyclone.data.vector

import io.sparkcyclone.annotations.VectorEngineTest
import io.sparkcyclone.data.conversion.SeqOptTConversions._
import io.sparkcyclone.data.{InputSamples, VeColVectorSource}
import io.sparkcyclone.vectorengine.WithVeProcess
import scala.util.Random
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

@VectorEngineTest
final class UnitColBatchUnitSpec extends AnyWordSpec with WithVeProcess {
  import io.sparkcyclone.util.CallContextOps._

  "UnitColBatch" should {
    s"correctly construct ${classOf[VeColBatch].getSimpleName} from Seq[Array[Byte]]" in {
      val size = Random.nextInt(100) + 100
      val input1 = InputSamples.seqOpt[Int](size)
      val input2 = InputSamples.seqOpt[Double](size)
      val input3 = InputSamples.seqOpt[String](size)

      val columns = Seq(
        input1.toBytePointerColVector("_"),
        input2.toBytePointerColVector("_"),
        input3.toBytePointerColVector("_"),
      )

      val arrays = columns.map(_.toBytes)
      val output = UnitColBatch(columns.map(_.toUnitColVector))
        .withData(arrays)
        .columns.map(_.toBytePointerColVector)

      output.size should be (3)
      output(0).toSeqOpt[Int] should be (input1)
      output(1).toSeqOpt[Double] should be (input2)
      output(2).toSeqOpt[String] should be (input3)
    }
  }
}
