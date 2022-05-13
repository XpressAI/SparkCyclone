package com.nec.vectorengine

import com.nec.colvector.{VeColVectorSource => VeSource}
import scala.collection.concurrent.{TrieMap => MMap}
import scala.util.Try
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.Duration
import com.codahale.metrics._
import com.typesafe.scalalogging.LazyLogging
import org.bytedeco.javacpp.{BytePointer, LongPointer, Pointer}
import org.bytedeco.veoffload.global.veo
import org.bytedeco.veoffload.{veo_args, veo_proc_handle, veo_thr_ctxt}

final case class WrappingVeo private (val node: Int,
                                      identifier: String,
                                      handle: veo_proc_handle,
                                      tcontext: veo_thr_ctxt,
                                      val metrics: MetricRegistry)
                                      extends VeProcess with LazyLogging {

  implicit class ExtendedPointer(buffer: Pointer) {
    def nbytes: Long = {
      buffer.limit * buffer.sizeof
    }
  }

  // Declare this prior to the logging statements or else the logging statements will fail
  private var opened = true

  // Internal allocation and library records for tracking
  private var heapRecords = MMap.empty[Long, VeAllocation]
  private var stackRecords = MMap.empty[Long, VeCallArgsStack]
  private var loadedLibRecords = MMap.empty[String, LibraryReference]

  // Internal process metrics
  private var syncFnCalls = 0L
  private var syncFnCallDurations = 0L  // In nanoseconds
  private val syncFnCallTimer = new Timer
  private val allocTimer = new Timer
  private val freeTimer = new Timer

  // Register the metrics with the MetricRegistry
  metrics.register(VeProcess.VeAllocDurationsMetric, allocTimer)
  metrics.register(VeProcess.VeFreeDurationsMetric, freeTimer)
  metrics.register(VeProcess.VeSyncFnCallDurationsMetric, syncFnCallTimer)
  metrics.register(VeProcess.NumAllocationsMetric, new Gauge[Long] {
      def getValue: Long = heapRecords.size
    }
  )
  metrics.register(VeProcess.BytesAllocatedMetric, new Gauge[Long] {
      def getValue: Long = heapRecords.valuesIterator.foldLeft(0L)(_ + _.size)
    }
  )
  metrics.register(VeProcess.VeSyncFnCallsCountMetric, new Gauge[Long] {
      def getValue: Long = syncFnCalls
    }
  )
  metrics.register(VeProcess.VeSyncFnCallTimesMetric, new Gauge[Double] {
      def getValue: Double = syncFnCallDurations / 1e6  // In milliseconds
    }
  )

  logger.info(s"Opened VE process (Node ${node}) @ ${handle.address}: ${handle}")
  logger.info(s"Opened VEO asynchronous context @ ${tcontext.address}: ${tcontext}")
  logger.info(s"VEO version ${version}; API version ${apiVersion}")

  private[vectorengine] def requireValidBufferForPut(buffer: Pointer): Unit = {
    require(buffer.address > 0L, s"Buffer has an invalid address ${buffer.address}; either it is un-initialized or already closed")
    require(buffer.nbytes > 0L, s"Buffer has a declared size of ${buffer.nbytes}; nothing to put to VE memory")
  }

  private[vectorengine] def requireValidBufferForGet(buffer: Pointer): Unit = {
    /*
      For `get()`s, there are situations where a 0-sized buffer is fetched,
      (e.g. the output of a filter on a column vector can be column vector of
      size 0), and so we don't require `buffer.nbytes` to be > 0L
    */
    require(buffer.address > 0L, s"Buffer has an invalid address ${buffer.address}; either it is un-initialized or already closed")
  }

  private[vectorengine] def withVeoProc[T](thunk: => T): T = {
    require(opened, "VE process is closed")
    thunk
  }

  private[vectorengine] def measureTime[T](thunk: => T): (T, Long) = {
    val start = System.nanoTime
    val result = thunk
    (result, System.nanoTime - start)
  }

  def isOpen: Boolean = {
    opened
  }

  lazy val apiVersion: Int = {
    withVeoProc {
      veo.veo_api_version
    }
  }

  lazy val version: String = {
    withVeoProc {
      val bytes = veo.veo_version_string
      bytes.getString(StandardCharsets.US_ASCII)
    }
  }

  lazy val source: VeSource = {
    VeSource(identifier)
  }

  def heapAllocations: Map[Long, VeAllocation] = {
    heapRecords.toMap
  }

  def stackAllocations: Map[Long, VeCallArgsStack] = {
    stackRecords.toMap
  }

  def loadedLibraries: Map[String, LibraryReference] = {
    loadedLibRecords.toMap
  }

  def allocate(size: Long): VeAllocation = {
    withVeoProc {
      require(size > 0L, s"Requested size ${size} is invalid")

      // Value is initialized to 0
      val ptr = new LongPointer(1)
      val (result, duration) = measureTime { veo.veo_alloc_mem(handle, ptr, size) }

      // Ensure memory is properly allocated
      val address = ptr.get
      require(result == 0, s"Memory allocation failed with code: ${result}")
      require(address > 0, s"Memory allocation returned an invalid address: ${ptr.get}")
      logger.trace(s"Allocated ${size} bytes ==> ${ptr}")
      ptr.close

      // Record metric
      allocTimer.update(Duration.ofNanos(duration))

      // Create an allocation record to track the allocation
      val allocation = VeAllocation(address, size, new Exception().getStackTrace)
      heapRecords.put(address, allocation)
      allocation
    }
  }

  def free(address: Long, unsafe: Boolean): Unit = {
    withVeoProc {
      require(address > 0L, s"Invalid VE memory address ${address}")

      heapRecords.get(address) match {
        case Some(allocation) =>
          logger.trace(s"Deallocating pointer @ ${address}")
          val (result, duration) = measureTime { veo.veo_free_mem(handle, address) }
          require(result == 0, s"Memory release failed with code: ${result}")
          // Remove only after the free() was successful
          heapRecords.remove(address)
          freeTimer.update(Duration.ofNanos(duration))

        case None if unsafe =>
          logger.warn(s"Releasing VE memory @ ${address} without safety checks!")
          val (result, duration) = measureTime { veo.veo_free_mem(handle, address) }
          require(result == 0, s"Memory release failed with code: ${result}")
          freeTimer.update(Duration.ofNanos(duration))

        case None =>
          throw new IllegalArgumentException(s"VE memory address does not correspond to a tracked allocation: ${address}")
      }
    }
  }

  def freeAll: Unit = {
    withVeoProc {
      logger.debug(s"Releasing all ${heapRecords.size} heap allocations held by the process")
      heapRecords.keys.foreach(free(_))

      logger.debug(s"Releasing all ${heapRecords.size} veo_args allocations held by the process")
      stackRecords.values.foreach(freeArgsStack)
    }
  }

  def put(buffer: Pointer): VeAllocation = {
    withVeoProc {
      requireValidBufferForPut(buffer)
      val allocation = allocate(buffer.nbytes)
      val result = veo.veo_write_mem(handle, allocation.address, buffer, buffer.nbytes)
      require(result == 0, s"veo_write_mem failed and returned ${result}")
      allocation
    }
  }

  def putAsync(buffer: Pointer): (VeAllocation, VeAsyncReqId) = {
    withVeoProc {
      requireValidBufferForPut(buffer)
      val allocation = allocate(buffer.nbytes)
      (allocation, putAsync(buffer, allocation.address))
    }
  }

  def putAsync(buffer: Pointer, destination: Long): VeAsyncReqId = {
    withVeoProc {
      requireValidBufferForPut(buffer)
      require(destination > 0L, s"Invalid VE memory address ${destination}")
      val id = veo.veo_async_write_mem(tcontext, destination, buffer, buffer.nbytes)
      require(id != veo.VEO_REQUEST_ID_INVALID, s"veo_async_write_mem failed and returned ${id}")
      VeAsyncReqId(id)
    }
  }

  def get(buffer: Pointer, source: Long): Unit = {
    withVeoProc {
      requireValidBufferForGet(buffer)
      require(source > 0L, s"Invalid VE memory address ${source}")
      val result = veo.veo_read_mem(handle, buffer, source, buffer.nbytes)
      require(result == 0, s"veo_read_mem failed and returned ${result}")
    }
  }

  def getAsync(buffer: Pointer, source: Long): VeAsyncReqId = {
    withVeoProc {
      requireValidBufferForGet(buffer)
      require(source > 0L, s"Invalid VE memory address ${source}")
      val id = veo.veo_async_read_mem(tcontext, buffer, source, buffer.nbytes)
      require(id != veo.VEO_REQUEST_ID_INVALID, s"veo_async_read_mem failed and returned ${id}")
      VeAsyncReqId(id)
    }
  }

  def peekResult(id: VeAsyncReqId): (Int, LongPointer) = {
    withVeoProc {
      val retp = new LongPointer(1)
      retp.put(Long.MinValue)
      val res = veo.veo_call_peek_result(tcontext, id.value, retp)
      (res, retp)
    }
  }

  def awaitResult(id: VeAsyncReqId): LongPointer = {
    withVeoProc {
      val retp = new LongPointer(1)
      retp.put(Long.MinValue)
      val res = veo.veo_call_wait_result(tcontext, id.value, retp)
      require(res == veo.VEO_COMMAND_OK, s"VE function returned value: ${res}")
      retp
    }
  }

  def load(path: Path): LibraryReference = {
    withVeoProc {
      val npath = path.normalize
      loadedLibRecords.get(npath.toString) match {
        case Some(lib) =>
          logger.debug(s"Library .SO has already been loaded: ${npath}")
          lib

        case None =>
          require(Files.exists(npath), s"Path does not correspond to an existing file: ${npath}")
          logger.info(s"Loading from path as .SO: ${npath}...")
          val result = veo.veo_load_library(handle, npath.toString)
          require(result > 0, s"Expected library reference to be > 0, got ${result} (library at: ${npath})")

          // Create a library load record to track
          val lib = LibraryReference(npath, result)
          loadedLibRecords.put(npath.toString, lib)
          lib
      }
    }
  }

  def unload(lib: LibraryReference): Unit = {
    withVeoProc {
      val npath = lib.path.normalize
      loadedLibRecords.get(npath.toString) match {
        case Some(lib) =>
          logger.info(s"Unloading library from the VE process: ${npath}...")
          val result = veo.veo_unload_library(handle, lib.value)
          require(result == 0, s"Failed to unload library from the VE process, got ${result} (library at: ${npath})")
          // Remove only after the veo_unload_library() was successful
          loadedLibRecords.remove(npath.toString)

        case None =>
          throw new IllegalArgumentException(s"VE process does not have library loaded; nothing to unload: ${npath}")
      }
    }
  }

  def getSymbol(lib: LibraryReference, name: String): LibrarySymbol = {
    withVeoProc {
      require(name.trim.nonEmpty, "Symbol name is empty or contains only whitespaces")
      val result = veo.veo_get_sym(handle, lib.value, name)
      require(result > 0, s"Expected > 0, but got ${result} when looking up symbol '${name}' (library at: ${lib.path})")
      LibrarySymbol(lib, name, result)
    }
  }

  def newArgsStack(inputs: Seq[CallStackArgument]): VeCallArgsStack = {
    withVeoProc {
      val args = veo.veo_args_alloc
      require(! args.isNull,  s"Fail to allocate arguments stack")
      logger.trace(s"Allocated veo_args @ ${args.address}")

      inputs.zipWithIndex.foreach {
        case (I32Arg(value), i) =>
          val result = veo.veo_args_set_i32(args, i, value)
          require(result == 0, s"Failed to set arguments stack at position ${i} to: ${value}")
          logger.trace(s"[veo_args @ ${args.address}] Insert @ position ${i}: ${value}")

        case (U64Arg(value), i) =>
          val result = veo.veo_args_set_u64(args, i, value)
          require(result == 0, s"Failed to set arguments stack at position ${i} to: ${value}")
          logger.trace(s"[veo_args @ ${args.address}] Insert @ position ${i}: ${value}")

        case (BuffArg(intent, buffer), i) =>
          val icode = intent match {
            case VeArgIntent.In     => veo.VEO_INTENT_IN
            case VeArgIntent.Out    => veo.VEO_INTENT_OUT
            case VeArgIntent.InOut  => veo.VEO_INTENT_INOUT
          }
          val result = veo.veo_args_set_stack(args, icode, i, new BytePointer(buffer), buffer.nbytes)
          require(result == 0, s"Failed to set arguments stack at position ${i} to: ${buffer}")
          logger.trace(s"[veo_args @ ${args.address}] Insert @ position ${i}: ${buffer.getClass.getSimpleName} buffer @ VH ${buffer.address} (${buffer.nbytes} bytes)")
      }

      // Create an allocation record to track the allocation
      val allocation = VeCallArgsStack(inputs, args)
      stackRecords.put(args.address, allocation)
      allocation
    }
  }

  def freeArgsStack(stack: VeCallArgsStack): Unit = {
    withVeoProc {
      stackRecords.get(stack.args.address) match {
        case Some(allocation) =>
          logger.trace(s"Releasing veo_args @ ${stack.args.address}")
          veo.veo_args_free(stack.args)
          // Remove only after the free() was successful
          stackRecords.remove(stack.args.address)

        case None =>
          throw new IllegalArgumentException(s"VeCallArgsStack does not correspond to a tracked veo_args allocation: ${stack.args.address}")
      }
    }
  }

  def call(func: LibrarySymbol, stack: VeCallArgsStack): LongPointer = {
    withVeoProc {
      logger.trace(s"Sync call '${func.name}' with veo_args @ ${stack.args.address}; argument values: ${stack.inputs}")

      // Set the output buffer
      val retp = new LongPointer(1)

      // Call the function
      val (result, duration) = measureTime { veo.veo_call_sync(handle, func.address, stack.args, retp) }

      // The VE call to the function should succeed
      require(
        result == 0,
        s"VE call failed for function '${func.name}' (library at: ${func.lib.path}); got ${result}"
      )

      // Record metrics
      syncFnCalls += 1
      syncFnCallDurations += duration
      syncFnCallTimer.update(Duration.ofNanos(duration))

      logger.debug(
        s"Finished call to '${func.name}': ${syncFnCalls} VeSeconds: (${syncFnCallDurations / 1e9} s)"
      )

      retp
    }
  }

  def callAsync(func: LibrarySymbol, stack: VeCallArgsStack): VeAsyncReqId = {
    withVeoProc {
      logger.trace(s"Async call '${func.name}' with veo_args @ ${stack.args.address}")

      val id = veo.veo_call_async(tcontext, func.address, stack.args)
      require(
        id != veo.VEO_REQUEST_ID_INVALID,
        s"VE async call failed for function '${func.name}' (library at: ${func.lib.path})"
      )

      VeAsyncReqId(id)
    }
  }

  def close: Unit = {
    if (opened) {
      val MaxToShow = 5

      // Complain about un-released heap allocations
      val hRecords = heapRecords.take(MaxToShow)
      if (hRecords.nonEmpty) {
        logger.error(s"There were some unreleased heap allocations. First ${MaxToShow}:")
        hRecords.foreach { case (_, record) =>
          logger.error(s"Position: ${record.address}", record.toThrowable)
        }
      }

      // Complain about un-released args stack allocations
      val sRecords = stackRecords.take(MaxToShow)
      if (sRecords.nonEmpty) {
        logger.error(s"There were some unreleased stack allocations. First ${MaxToShow}:")
        sRecords.foreach { case (_, record) =>
          logger.error(s"Position: ${record.args.address}")
        }
      }

      Try {
        logger.info(s"Closing VEO asynchronous context @ ${tcontext.address}")
        veo.veo_context_close(tcontext)

        logger.info(s"Closing VE process (Node ${node}) @ ${handle.address}")
        veo.veo_proc_destroy(handle)

        logger.info(s"Clearing allocation records held by the VE process")
        heapRecords.clear

        logger.info(s"Clearing veo_args allocations records held by the VE process")
        stackRecords.clear

        logger.info(s"Clearing loaded libraries records held by the VE process")
        loadedLibRecords.clear
      }

      opened = false
    }
  }
}