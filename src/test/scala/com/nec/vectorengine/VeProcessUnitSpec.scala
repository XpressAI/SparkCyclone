package com.nec.vectorengine

import com.nec.cyclone.annotations.VectorEngineTest
import scala.util.Random
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.BeforeAndAfterAll

@VectorEngineTest
final class VeProcessUnitSpec extends AnyWordSpec with BeforeAndAfterAll {
  val process = VeProcess.create(getClass.getName)

  override def afterAll(): Unit = {
    process.close
  }

  "VeProcess" should {
    "correctly contain the node number where the VE process was allocated" in {
      process.node should be >= 0
    }

    "correctly return the API version number" in {
      process.apiVersion should be > 0
    }

    "correctly return a version string" in {
      process.version shouldNot be (empty)
    }

    "correctly allocate and free memory" in {
      val size = Random.nextInt(1000) + 100

      noException should be thrownBy {
        val location = process.allocate(size)
        location should be > 0L
        process.free(location)
      }
    }
    // "NOT crash if a double-free were called" in {
    //   val colvec1 = InputSamples.seqOpt[Int].toBytePointerColVector("_").toVeColVector
    //   val colvec2 = InputSamples.seqOpt[Double].toBytePointerColVector("_").toVeColVector
    //   val colvec3 = InputSamples.seqOpt[String].toBytePointerColVector("_").toVeColVector

    //   noException should be thrownBy {
    //     // Should call at least twice
    //     0.to(Random.nextInt(3) + 1).foreach(_ => colvec1.free)
    //     0.to(Random.nextInt(3) + 1).foreach(_ => colvec2.free)
    //     0.to(Random.nextInt(3) + 1).foreach(_ => colvec3.free)
    //   }
    // }
  }
}
