package com.nec.colvector

import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.colvector.SeqOptTConversions._
import com.nec.util.CallContext
import com.nec.ve.WithVeProcess
import com.nec.vectorengine.{WithVeProcess => WithNewVeProcess}
import scala.reflect.ClassTag
import scala.util.Random
import java.util.UUID
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers._

@VectorEngineTest
final class VeColVectorUnitSpec extends AnyWordSpec with WithVeProcess {
  import com.nec.util.CallContextOps._

  def runTransferTest(input: BytePointerColVector): BytePointerColVector = {
    val colvec = input.toVeColVector

    colvec.veType should be (input.veType)
    colvec.name should be (input.name)
    colvec.source should be (input.source)
    colvec.numItems should be (input.numItems)

    colvec.toBytePointerColVector
  }

  "VeColVector" should {
    "correctly transfer data from Host Off-Heap to VE and back (Int)" in {
      val input = InputSamples.seqOpt[Int]
      runTransferTest(input.toBytePointerColVector("_")).toSeqOpt[Int] should be (input)
    }

    "correctly transfer data from Host Off-Heap to VE and back (Short)" in {
      val input = InputSamples.seqOpt[Short]
      runTransferTest(input.toBytePointerColVector("_")).toSeqOpt[Short] should be (input)
    }

    "correctly transfer data from Host Off-Heap to VE and back (Long)" in {
      val input = InputSamples.seqOpt[Long]
      runTransferTest(input.toBytePointerColVector("_")).toSeqOpt[Long] should be (input)
    }

    "correctly transfer data from Host Off-Heap to VE and back (Float)" in {
      val input = InputSamples.seqOpt[Float]
      runTransferTest(input.toBytePointerColVector("_")).toSeqOpt[Float] should be (input)
    }

    "correctly transfer data from Host Off-Heap to VE and back (Double)" in {
      val input = InputSamples.seqOpt[Double]
      runTransferTest(input.toBytePointerColVector("_")).toSeqOpt[Double] should be (input)
    }

    "correctly transfer data from Host Off-Heap to VE and back (String)" in {
      val input = InputSamples.seqOpt[String]
      runTransferTest(input.toBytePointerColVector("_")).toSeqOpt[String] should be (input)
    }
  }
}

@VectorEngineTest
final class NewVeColVectorUnitSpec extends AnyWordSpec with WithNewVeProcess {
  import com.nec.util.CallContextOps._

  def runTransferTest(input: BytePointerColVector): BytePointerColVector = {
    val colvec = input.toVeColVector2

    colvec.veType should be (input.veType)
    colvec.name should be (input.name)
    colvec.source should be (input.source)
    colvec.numItems should be (input.numItems)

    colvec.toBytePointerColVector2
  }

  "VeColVector" should {
    "correctly transfer data from Host Off-Heap to VE and back (Int)" in {
      val input = InputSamples.seqOpt[Int]
      runTransferTest(input.toBytePointerColVector("_")).toSeqOpt[Int] should be (input)
    }

    "correctly transfer data from Host Off-Heap to VE and back (Short)" in {
      val input = InputSamples.seqOpt[Short]
      runTransferTest(input.toBytePointerColVector("_")).toSeqOpt[Short] should be (input)
    }

    "correctly transfer data from Host Off-Heap to VE and back (Long)" in {
      val input = InputSamples.seqOpt[Long]
      runTransferTest(input.toBytePointerColVector("_")).toSeqOpt[Long] should be (input)
    }

    "correctly transfer data from Host Off-Heap to VE and back (Float)" in {
      val input = InputSamples.seqOpt[Float]
      runTransferTest(input.toBytePointerColVector("_")).toSeqOpt[Float] should be (input)
    }

    "correctly transfer data from Host Off-Heap to VE and back (Double)" in {
      val input = InputSamples.seqOpt[Double]
      runTransferTest(input.toBytePointerColVector("_")).toSeqOpt[Double] should be (input)
    }

    "correctly transfer data from Host Off-Heap to VE and back (String)" in {
      val input = InputSamples.seqOpt[String]
      runTransferTest(input.toBytePointerColVector("_")).toSeqOpt[String] should be (input)
    }
  }
}
