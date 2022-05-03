package com.nec.vectorengine

import com.nec.cyclone.annotations.VectorEngineTest
import scala.concurrent.duration._
import scala.util.Random
import org.bytedeco.javacpp.{BytePointer, LongPointer}
import org.bytedeco.veoffload.global.veo
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.BeforeAndAfterAll

@VectorEngineTest
final class VeProcessUnitSpec extends AnyWordSpec with BeforeAndAfterAll with Eventually {
  override implicit val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(3.seconds),
    interval = scaled(0.5.seconds)
  )

  val process = VeProcess.create(getClass.getName)

  override def afterAll(): Unit = {
    process.close

    // Calling close twice should not break
    noException should be thrownBy {
      process.close
    }

    process.isOpen should be (false)
  }

  def randomBytes(size: Int): Array[Byte] = {
    val bytes = Array.ofDim[Byte](size.abs)
    Random.nextBytes(bytes)
    bytes
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

    "handle the case where zero memory is requested for allocation" in {
      intercept[IllegalArgumentException] {
        process.allocate(0L)
      }
    }

    "handle the case where an invalid VE address is requested to be freed" in {
      intercept[IllegalArgumentException] {
        process.free(- Random.nextInt(1000).toLong)
      }
    }

    "correctly transfer data from VH to VE and back (sync)" in {
      noException should be thrownBy {
        val bytes1 = randomBytes(Random.nextInt(10000) + 10)
        val bytes2 = Array.ofDim[Byte](bytes1.size)

        // Bytes should not be equal right now
        bytes1.toSeq should not be (bytes2.toSeq)

        // Move from JVM to VH
        val buffer1 = new BytePointer(bytes1.size)
        buffer1.put(bytes1, 0, bytes1.size)

        // Move from VH to VE
        val location = process.put(buffer1)
        location should be > 0L

        // Move from VE to VH
        val buffer2 = process.get(location, bytes2.size)

        // Move from VH to JVM
        buffer2.get(bytes2)

        // Bytes should be the same
        bytes1.toSeq should be (bytes2.toSeq)
      }
    }

    "handle corner cases when transferring data from VH to VE (sync) (i.e. should not crash the JVM)" in {
      // BytePointer is closed
      intercept[IllegalArgumentException] {
        val buffer = new BytePointer(Random.nextInt(10000))
        buffer.close
        process.put(buffer)
      }

      // BytePointer has size 0
      intercept[IllegalArgumentException] {
        val buffer = new BytePointer(0)
        buffer.close
        process.put(buffer)
      }
    }

    "handle corner cases when transferring data from VE to VH (sync) (i.e. should not crash the JVM)" in {
      // VE memory location is invalid
      intercept[IllegalArgumentException] {
        process.get(- Random.nextInt(10000).toLong, Random.nextInt(10000).toLong + 10)
      }

      // Fetch size is invalid
      intercept[IllegalArgumentException] {
        process.get(Random.nextInt(10000).toLong + 1, - Random.nextInt(10000).toLong)
      }
    }

    "correctly transfer data from VH to VE and back (async)" in {
      noException should be thrownBy {
        val bytes1 = randomBytes(Random.nextInt(100000) + 10)
        val bytes2 = Array.ofDim[Byte](bytes1.size)

        // Bytes should not be equal right now
        bytes1.toSeq should not be (bytes2.toSeq)

        // Move from JVM to VH
        val buffer1 = new BytePointer(bytes1.size)
        buffer1.put(bytes1, 0, bytes1.size)

        // Move from VH to VE (async)
        val (location, id1) = process.putAsync(buffer1)
        location should be > 0L

        // Fetch the async transfer result using `peek`
        eventually {
          val (res, xx) = process.peekResult(id1)
          res should be (veo.VEO_COMMAND_OK)
        }

        // Move from VE to VH (async)
        val buffer2 = new BytePointer(bytes2.size)
        val id2 = process.getAsync(buffer2, location)

        // Fetch the async transfer result using `await`
        process.awaitResult(id2)

        // Move from VH to JVM
        buffer2.get(bytes2)

        // Bytes should be the same
        bytes1.toSeq should be (bytes2.toSeq)
      }
    }

    "handle corner cases when transferring data from VH to VE (async) (i.e. should not crash the JVM)" in {
      // BytePointer is closed
      intercept[IllegalArgumentException] {
        val buffer = new BytePointer(Random.nextInt(10000))
        buffer.close
        process.putAsync(buffer)
      }

      // BytePointer has size 0
      intercept[IllegalArgumentException] {
        val buffer = new BytePointer(0)
        buffer.close
        process.putAsync(buffer)
      }

      // VE memory address is invalid
      intercept[IllegalArgumentException] {
        val buffer = new BytePointer(Random.nextInt(10000))
        buffer.close
        process.putAsync(buffer, - Random.nextInt(10000))
      }
    }

    "handle corner cases when transferring data from VE to VH (async) (i.e. should not crash the JVM)" in {
      // BytePointer is closed
      intercept[IllegalArgumentException] {
        val buffer = new BytePointer(Random.nextInt(10000))
        buffer.close
        process.getAsync(buffer, Random.nextInt(10000) + 100)
      }

      // BytePointer has size 0
      intercept[IllegalArgumentException] {
        val buffer = new BytePointer(0)
        buffer.close
        process.getAsync(buffer, Random.nextInt(10000) + 100)
      }

      // VE memory address is invalid
      intercept[IllegalArgumentException] {
        val buffer = new BytePointer(Random.nextInt(10000))
        process.putAsync(buffer, - Random.nextInt(10000))
      }
    }

    "correctly allocate and free a veo_args" in {
      noException should be thrownBy {
        val buffer = new LongPointer(Random.nextInt(4) + 1)
        val inputs = Seq(
          I32Arg(Random.nextInt),
          U64Arg(Random.nextLong),
          BuffArg(VeArgIntent.Out, buffer)
        )

        // Create the args stack with the veo_args
        val stack = process.newArgsStack(inputs)
        stack.inputs should be (inputs)

        // Free the args stack
        process.freeArgsStack(stack)
      }
    }
  }
}
