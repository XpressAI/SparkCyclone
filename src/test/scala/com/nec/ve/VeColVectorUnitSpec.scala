package com.nec.ve

import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.colvector.SeqOptTConversions._
import com.nec.colvector.VeColBatch.VeColVectorSource
import com.nec.ve.VeProcess.OriginalCallingContext
import scala.reflect.ClassTag
import scala.util.Random
import java.util.UUID
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers._

@VectorEngineTest
final class VeColVectorUnitSpec extends AnyWordSpec with WithVeProcess {
  import OriginalCallingContext.Automatic._

  def runConversionTest[T <: AnyVal : ClassTag](input: Seq[Option[T]]): Unit = {
    implicit val source = VeColVectorSource(s"${UUID.randomUUID}")
    val name = s"${UUID.randomUUID}"
    val colvec = input.toBytePointerColVector(name).toVeColVector

    // Check fields
    colvec.veType.scalaType should be (implicitly[ClassTag[T]].runtimeClass)
    colvec.name should be (name)
    colvec.source should be (source)
    colvec.numItems should be (input.size)
    colvec.buffers.size should be (2)

    // Check conversion
    colvec.toBytePointerVector.toSeqOpt[T] should be (input)
  }

  "VeColVector" should {
    "correctly transfer data from Host Off-Heap to VE and back (Int)" in {
      runConversionTest(0.until(Random.nextInt(100)).map(_ => if (Math.random < 0.5) Some(Random.nextInt(10000)) else None))
    }

    "correctly transfer data from Host Off-Heap to VE and back (Short)" in {
      runConversionTest(0.until(Random.nextInt(100)).map(_ => if (Math.random < 0.5) Some(Random.nextInt(10000).toShort) else None))
    }

    "correctly transfer data from Host Off-Heap to VE and back (Long)" in {
      runConversionTest(0.until(Random.nextInt(100)).map(_ => if (Math.random < 0.5) Some(Random.nextLong) else None))
    }

    "correctly transfer data from Host Off-Heap to VE and back (Float)" in {
      runConversionTest(0.until(Random.nextInt(100)).map(_ => if (Math.random < 0.5) Some(Random.nextFloat * 1000) else None))
    }

    "correctly transfer data from Host Off-Heap to VE and back (Double)" in {
      runConversionTest(0.until(Random.nextInt(100)).map(_ => if (Math.random < 0.5) Some(Random.nextDouble * 1000) else None))
    }

    "correctly transfer data from Host Off-Heap to VE and back (String)" in {
      val input = 0.until(Random.nextInt(100)).map(_ => if (Math.random < 0.5) Some(Random.nextString(Random.nextInt(30))) else None)

      implicit val source = VeColVectorSource(s"${UUID.randomUUID}")
      val name = s"${UUID.randomUUID}"
      val colvec = input.toBytePointerColVector(name).toVeColVector

      // Check fields
      colvec.veType.scalaType should be (classOf[String])
      colvec.name should be (name)
      colvec.source should be (source)
      colvec.numItems should be (input.size)
      colvec.buffers.size should be (4)


      // Check conversion
      colvec.toBytePointerVector.toSeqOpt[String] should be (input)
    }
  }
}
