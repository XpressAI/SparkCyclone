package com.nec.vectorengine

import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.colvector.InputSamples
import com.nec.ve.VeKernelInfra
import scala.concurrent.duration._
import scala.util.Random
import java.io.File
import java.nio.file.FileSystems
import java.util.UUID
import org.bytedeco.javacpp.{BytePointer, DoublePointer, LongPointer}
import org.bytedeco.veoffload.global.veo
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

@VectorEngineTest
final class VeProcessUnitSpec extends AnyWordSpec with BeforeAndAfterAll with Eventually with VeKernelInfra {
  override implicit val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(3.seconds),
    interval = scaled(0.5.seconds)
  )

  // Don't open the process here because the ScalaTest classes are initialized
  var process: VeProcess = _

  override def beforeAll: Unit = {
    process = VeProcess.create(getClass.getName)
  }

  override def afterAll: Unit = {
    // Free all unreleased allocations and close the process
    process.freeAll
    process.close

    // Calling close twice should not break
    noException should be thrownBy {
      process.close
    }

    // Allocation and library records should be deleted on close
    process.heapAllocations shouldBe empty
    process.stackAllocations shouldBe empty
    process.loadedLibraries shouldBe empty

    // Attempting to do anything with a closed process should fail
    intercept[IllegalArgumentException] {
      process.allocate(Random.nextInt(10000) + 100)
    }

    // VE process should now be indicated as closed
    process.isOpen should be (false)
  }

  "VeProcess" should {
    "be able to load the Cyclone C++ Library" in {
      noException should be thrownBy {
        // Load libcyclone
        val lib = process.load(LibCyclone.SoPath)

        // Unload libcyclone
        process.unload(lib)
      }
    }

    "correctly indicate that the underlying VE process is open" in {
      process.isOpen should be (true)
    }

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
      val size = Random.nextInt(10000) + 100

      // Tracker should be empty
      process.heapAllocations shouldBe empty

      // Metrics should be zero
      process.metrics.getGauges.get(VeProcess.NumTrackedAllocationsMetric).getValue should be (0L)
      process.metrics.getGauges.get(VeProcess.TrackBytesAllocatedMetric).getValue should be (0L)
      process.metrics.getTimers.get(VeProcess.VeAllocTimerMetric).getCount should be (0L)
      process.metrics.getTimers.get(VeProcess.VeFreeTimerMetric).getCount should be (0L)

      val allocation = process.allocate(size)
      allocation.address should be > 0L

      // Tracker should now contain the record
      process.heapAllocations should not be empty
      process.heapAllocations.keys should be (Set(allocation.address))

      // Metrics should be non-zero
      process.metrics.getGauges.get(VeProcess.NumTrackedAllocationsMetric).getValue should be (1L)
      process.metrics.getGauges.get(VeProcess.TrackBytesAllocatedMetric).getValue should be (size.toLong)
      process.metrics.getTimers.get(VeProcess.VeAllocTimerMetric).getCount should be (1L)
      process.metrics.getTimers.get(VeProcess.VeFreeTimerMetric).getCount should be (0L)

      // First call to `free()` should work
      noException should be thrownBy {
        process.free(allocation.address)
      }

      // Tracker should be back to empty
      process.heapAllocations shouldBe empty

      // Metrics should be back to zero
      process.metrics.getGauges.get(VeProcess.NumTrackedAllocationsMetric).getValue should be (0L)
      process.metrics.getGauges.get(VeProcess.TrackBytesAllocatedMetric).getValue should be (0L)
      process.metrics.getTimers.get(VeProcess.VeAllocTimerMetric).getCount should be (1L)
      process.metrics.getTimers.get(VeProcess.VeFreeTimerMetric).getCount should be (1L)

      // Double `free()` should just log error without running and crashing the JVM
      noException should be thrownBy {
        process.free(allocation.address)
      }
    }

    "handle the case where an invalid memory size is requested for allocation" in {
      // Zero requested allocation size
      intercept[IllegalArgumentException] {
        process.allocate(0L)
      }

      // Negative requested allocation size
      intercept[IllegalArgumentException] {
        process.allocate(- Random.nextInt(10000).toLong)
      }
    }

    "handle the case where an invalid VE address is requested to be freed" in {
      // VE memory address is invalid
      intercept[IllegalArgumentException] {
        process.free(- Random.nextInt(10000).toLong)
      }

      // VE memory address is valid but has never been allocated before
      noException should be thrownBy {
        process.free(Random.nextInt(10000).toLong)
      }
    }

    "correctly register, de-register, and re-register memory allocations for tracking" in {
      val size = Random.nextInt(10000) + 100

      // Allocate
      val allocation = process.allocate(size)

      // Tracker should contain the record
      process.heapAllocations.keys should be (Set(allocation.address))

      // VE memory address is invalid (< 0)
      intercept[IllegalArgumentException] {
        process.registerAllocation(- (Random.nextInt(10000).toLong + 1), Random.nextInt(10000) + 100)
      }

      // Allocation size is invalid (< 0)
      intercept[IllegalArgumentException] {
        process.registerAllocation(Random.nextInt(10000) + 100, - (Random.nextInt(10000).toLong + 1))
      }

      // VE memory address is invalid (< 0)
      intercept[IllegalArgumentException] {
        process.unregisterAllocation(- (Random.nextInt(10000).toLong + 1))
      }

      // Register a conflicting allocation (same address, different size)
      intercept[IllegalArgumentException] {
        process.registerAllocation(allocation.address, allocation.size + Random.nextInt(10000).toLong + 100)
      }

      // Registering the same allocation should be a no-op
      noException should be thrownBy {
        process.registerAllocation(allocation.address, allocation.size) should be (allocation)
      }

      // Un-register allocation from tracking
      noException should be thrownBy {
        process.unregisterAllocation(allocation.address)
        process.heapAllocations shouldBe empty
      }

      // Re-register allocation for tracking
      noException should be thrownBy {
        process.registerAllocation(allocation.address, allocation.size)
        process.heapAllocations.keys should be (Set(allocation.address))
      }

      // Free should work
      noException should be thrownBy {
        process.free(allocation.address)
        process.heapAllocations shouldBe empty
      }

      // Free on address 0 should always work
      noException should be thrownBy {
        0.to(Random.nextInt(10)).foreach { _ => process.free(0L) }
      }

      // Unregister allocation on address 0 should always work
      noException should be thrownBy {
        0.to(Random.nextInt(10)).foreach { _ => process.unregisterAllocation(0L) }
      }
    }

    "correctly transfer Array[Byte] from VH to VE and back (sync)" in {
      noException should be thrownBy {
        val bytes1 = InputSamples.array[Byte](Random.nextInt(10000) + 10)
        val bytes2 = Array.ofDim[Byte](bytes1.size)

        // Bytes should not be equal right now
        bytes1.toSeq should not be (bytes2.toSeq)

        // Move from JVM to VH
        val buffer1 = new BytePointer(bytes1.size)
        buffer1.put(bytes1, 0, bytes1.size)

        // Move from VH to VE
        val allocation = process.put(buffer1)
        allocation.address should be > 0L

        // Move from VE to VH
        val buffer2 = new BytePointer(bytes2.size)
        process.get(buffer2, allocation.address)

        // Move from VH to JVM
        buffer2.get(bytes2)

        // Bytes should be the same
        bytes1.toSeq should be (bytes2.toSeq)
      }
    }

    "correctly transfer Array[Double] from VH to VE and back (sync)" in {
      noException should be thrownBy {
        val array1 = InputSamples.array[Double](Random.nextInt(10000) + 10)
        val array2 = Array.ofDim[Double](array1.size)

        // Bytes should not be equal right now
        array1.toSeq should not be (array2.toSeq)

        // Move from JVM to VH
        val buffer1 = new DoublePointer(array1.size)
        buffer1.put(array1, 0, array1.size)

        // Move from VH to VE
        val allocation = process.put(buffer1)
        allocation.address should be > 0L

        // Move from VE to VH
        val buffer2 = new DoublePointer(array2.size)
        process.get(buffer2, allocation.address)

        // Move from VH to JVM
        buffer2.get(array2)

        // Bytes should be the same
        array1.toSeq should be (array2.toSeq)
      }
    }

    "handle corner cases when transferring data from VH to VE (sync) (i.e. should not crash the JVM)" in {
      // BytePointer is closed
      intercept[IllegalArgumentException] {
        val buffer = new BytePointer(Random.nextInt(10000))
        buffer.close
        process.put(buffer)
      }

      // BytePointer buffer has size 0
      intercept[IllegalArgumentException] {
        val buffer = new BytePointer(0)
        buffer.close
        process.put(buffer)
      }
    }

    "handle corner cases when transferring data from VE to VH (sync) (i.e. should not crash the JVM)" in {
      // BytePointer is closed
      intercept[IllegalArgumentException] {
        val buffer = new BytePointer(Random.nextInt(10000))
        buffer.close
        process.get(buffer, Random.nextInt(10000) + 100)
      }

      // BytePointer buffer has size 0
      intercept[IllegalArgumentException] {
        val buffer = new BytePointer(0)
        buffer.close
        process.get(buffer, Random.nextInt(10000) + 100)
      }

      // VE memory address is invalid
      intercept[IllegalArgumentException] {
        val buffer = new BytePointer(Random.nextInt(10000))
        process.get(buffer, - Random.nextInt(10000))
      }
    }

    "correctly transfer data from VH to VE and back (async)" in {
      noException should be thrownBy {
        val bytes1 = InputSamples.array[Byte](Random.nextInt(100000) + 10)
        val bytes2 = Array.ofDim[Byte](bytes1.size)

        // Bytes should not be equal right now
        bytes1.toSeq should not be (bytes2.toSeq)

        // Move from JVM to VH
        val buffer1 = new BytePointer(bytes1.size)
        buffer1.put(bytes1, 0, bytes1.size)

        // Move from VH to VE (async)
        val (allocation, id1) = process.putAsync(buffer1)
        allocation.address should be > 0L

        // Fetch the async transfer result using `peek`
        eventually {
          val (res, retp) = process.peekResult(id1)
          res should be (veo.VEO_COMMAND_OK)
          retp.get should be (0L)
        }

        // Move from VE to VH (async)
        val buffer2 = new BytePointer(bytes2.size)
        val id2 = process.getAsync(buffer2, allocation.address)

        // Fetch the async transfer result using `await`
        process.awaitResult(id2).get should be (0L)

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

      // BytePointer buffer has size 0
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

      // BytePointer buffer has size 0
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

    "correctly create and release a `veo_args`" in {
      noException should be thrownBy {
        val buffer = new LongPointer(Random.nextInt(4) + 1)
        val inputs = Seq(
          I32Arg(Random.nextInt),
          U64Arg(Random.nextLong),
          BuffArg(VeArgIntent.Out, buffer)
        )

        // Tracker should be empty
        process.stackAllocations shouldBe empty

        // Create the args stack with the veo_args
        val stack = process.newArgsStack(inputs)
        stack.inputs should be (inputs)

        // Tracker should now be be non-empty
        process.stackAllocations should not be empty
        process.stackAllocations.keys should be (Set(stack.args.address))

        // Free the args stack
        process.freeArgsStack(stack)

        // Tracker should be back to empty
        process.stackAllocations shouldBe empty
      }
    }

    "handle the case of loading a non-existent or invalid .SO file" in {
      intercept[IllegalArgumentException] {
        // Non-existent path
        process.load(FileSystems.getDefault.getPath(s"/${UUID.randomUUID}/${UUID.randomUUID}"))
      }

      val path = File.createTempFile("tmp-",".so").toPath
      intercept[IllegalArgumentException] {
        // Invalid .SO file
        process.load(path)
      }
    }

    "handle the case of looking up a whitespace or non-existent symbol in a loaded library" in {
      val fnName = "func"
      val code = s"""
        | extern "C" double ${fnName} (double input) {
        |   return input * 2.0;
        | }
        """.stripMargin

      withCompiled(code) { path =>
        // Libraries tracker should be empty
        process.loadedLibraries shouldBe empty

        // Load the library
        val library = process.load(path)

        // Path should be saved in the library reference as a string
        library.path should be (path.normalize.toString)

        // Libraries tracker should now contain the record
        process.loadedLibraries should be (Map(path.normalize.toString -> library))

        // Symbol with only whitespaces
        intercept[IllegalArgumentException] {
          process.getSymbol(library, "   ")
        }

        // Non-existent symbol
        intercept[IllegalArgumentException] {
          process.getSymbol(library, s"${UUID.randomUUID}")
        }

        noException should be thrownBy {
          val func = process.getSymbol(library, fnName)
          func.address should be > 0L
        }

        // Unload the library
        process.unload(library)

        // Libraries tracker should be back to empty
        process.loadedLibraries shouldBe empty
      }
    }

    "correctly load a library and execute a function call (sync)" in {
      val fnName = s"func_${Random.nextInt(10)}"
      val retval = Random.nextLong

      // Define a function that multplies the input values by 2
      val code = s"""
        | #include <stdlib.h>
        |
        | extern "C" long ${fnName} (double *inputs, size_t size, double **outputs) {
        |   *outputs = static_cast<double *>(malloc(sizeof(double) * size));
        |
        |   for (auto i = 0; i < size; i++) {
        |     (*outputs)[i] = inputs[i] * 2;
        |   }
        |
        |   return ${retval};
        | }
        """.stripMargin

      // Set up the input and output buffers on JVM
      val input = InputSamples.array[Double](Random.nextInt(10000) + 10)
      val output = Array.ofDim[Double](input.size)

      // Move input to VH
      val inbuffer = new DoublePointer(input.size)
      inbuffer.put(input, 0, input.size)

      // Set up the output pointer with an initial value
      val outptr = new LongPointer(1)
      outptr.put(-99)

      // Set up the arguments stack
      val stack = process.newArgsStack(Seq(
        BuffArg(VeArgIntent.In, inbuffer),
        U64Arg(input.size.toLong),
        BuffArg(VeArgIntent.Out, outptr)
      ))

      withCompiled(code) { path =>
        // Load the library
        val library = process.load(path)

        // Locate the function symbol
        val func = process.getSymbol(library, fnName)

        // Metrics should be zero
        process.metrics.getGauges.get(VeProcess.VeSyncFnCallsCountMetric).getValue should be (0L)
        process.metrics.getGauges.get(VeProcess.VeSyncFnCallTimesMetric).getValue should be (0L)
        process.metrics.getTimers.get(VeProcess.VeSyncFnCallTimerMetric).getCount should be (0L)

        // Call the function (sync)
        val retp = process.call(func, stack)
        retp.get should be (retval)

        // Metrics should be non-zero
        process.metrics.getGauges.get(VeProcess.VeSyncFnCallsCountMetric).getValue should be (1L)
        process.metrics.getGauges.get(VeProcess.VeSyncFnCallTimesMetric).getValue shouldNot be (0L)
        process.metrics.getTimers.get(VeProcess.VeSyncFnCallTimerMetric).getCount should be (1L)

        // Dereference the output pointer and move output from VE to VH
        val outbuffer = new DoublePointer(input.size)
        process.get(outbuffer, outptr.get)

        // Move output from VH to JVM
        outbuffer.get(output)

        // Output values should be the input values doubled
        output.toSeq should be (input.toSeq.map(_ * 2))

        // Perform "unsafe free" on the memory allocated inside the VE function call - should not fail or crash the JVM
        noException should be thrownBy {
          process.free(outptr.get, unsafe = true)
        }

        // Unload the library
        process.unload(library)
      }
    }

    "correctly load a library and execute a function call (async)" in {
      val fnName = s"func_${Random.nextInt(10)}"
      // Need to return a positive value for peek and await to not throw exception
      val retval = Random.nextLong.abs

      // Define a function that takes the input values to the second power
      val code = s"""
        | #include <stddef.h>
        |
        | extern "C" long ${fnName} (double *inputs, size_t size, double **outputs) {
        |   *outputs = new double[size];
        |
        |   for (auto i = 0; i < size; i++) {
        |     (*outputs)[i] = inputs[i] * inputs[i];
        |   }
        |
        |   return ${retval};
        | }
        """.stripMargin

      // Set up the input and output buffers on JVM
      val input = InputSamples.array[Double](Random.nextInt(10000) + 10)
      val output = Array.ofDim[Double](input.size)

      // Move input to VH
      val inbuffer = new DoublePointer(input.size)
      inbuffer.put(input, 0, input.size)

      // Set up the output pointer with an initial value
      val outptr = new LongPointer(1)
      outptr.put(-99)

      // Set up the arguments stack
      val stack = process.newArgsStack(Seq(
        BuffArg(VeArgIntent.In, inbuffer),
        U64Arg(input.size.toLong),
        BuffArg(VeArgIntent.Out, outptr)
      ))

      withCompiled(code) { path =>
        // Load the library
        val library = process.load(path)

        // Locate the function symbol
        val func = process.getSymbol(library, fnName)

        // Call the function (async)
        val id1 = process.callAsync(func, stack)

        // Wait for the function call to complete using `await`
        process.awaitResult(id1).get should be (retval)

        // Dereference the output pointer and move output from VE to VH
        val outbuffer = new DoublePointer(input.size)
        process.get(outbuffer, outptr.get)

        // Move output from VH to JVM
        outbuffer.get(output)

        // Output values should be the input values taken to the second power
        output.toSeq should be (input.toSeq.map(x => x * x))

        // Unload the library
        process.unload(library)
      }
    }
  }
}
