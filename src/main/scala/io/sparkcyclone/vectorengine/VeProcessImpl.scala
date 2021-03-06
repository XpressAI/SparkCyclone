package io.sparkcyclone.vectorengine

import io.sparkcyclone.data.{VeColVectorSource => VeSource}
import io.sparkcyclone.util.PointerOps._
import scala.collection.concurrent.{TrieMap => TMap}
import scala.collection.mutable.{Map => MMap}
import scala.util.Try
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.Duration
import java.util.concurrent.locks.ReentrantReadWriteLock
import com.codahale.metrics._
import com.typesafe.scalalogging.LazyLogging
import org.bytedeco.javacpp.{BytePointer, LongPointer, Pointer}
import org.bytedeco.veoffload.global.veo
import org.bytedeco.veoffload.{veo_proc_handle, veo_thr_ctxt}

final case class WrappingVeo private (val node: Int,
                                      identifier: String,
                                      handle: veo_proc_handle,
                                      tcontexts: Seq[veo_thr_ctxt],
                                      val metrics: MetricRegistry)
                                      extends VeProcess with LazyLogging {
  require(handle.address >= 0, s"Invalid ${classOf[veo_proc_handle].getSimpleName} address was provided; cannot initialize ${classOf[WrappingVeo].getSimpleName}")
  require(tcontexts.nonEmpty, s"[${handle.address}] No asynchronous VEO context was provided")

  // Declare this prior to the logging statements or else the logging statements will fail
  private var opened = true
  private val openlock = new ReentrantReadWriteLock(true)

  // VEO async thread context locks
  private val contextLocks: Seq[(ReentrantReadWriteLock, veo_thr_ctxt)] = tcontexts.map(new ReentrantReadWriteLock(true) -> _)

  // Reference to libcyclone.so
  private var libCyclone: LibraryReference = _

  // Internal allocation and library records for tracking
  private var heapRecords = TMap.empty[Long, VeAllocation]
  private var stackRecords = MMap.empty[Long, VeCallArgsStack]
  private var loadedLibRecords = MMap.empty[String, LibraryReference]

  // Internal process metrics
  private var syncFnCalls = 0L
  private var syncFnCallDurations = 0L  // In nanoseconds
  private val syncFnCallTimer = new Timer
  private val allocTimer = new Timer
  private val freeTimer = new Timer
  private val allocSizesHistogram = new Histogram(new UniformReservoir)
  private val putSizesHistogram = new Histogram(new UniformReservoir)
  private val putThroughputsHistogram = new Histogram(new UniformReservoir)

  // Register the metrics with the MetricRegistry
  metrics.register(VeProcess.VeAllocTimerMetric,            allocTimer)
  metrics.register(VeProcess.VeFreeTimerMetric,             freeTimer)
  metrics.register(VeProcess.VeSyncFnCallTimerMetric,       syncFnCallTimer)
  metrics.register(VeProcess.AllocSizesHistogramMetric,     allocSizesHistogram)
  metrics.register(VeProcess.PutSizesHistogramMetric,       putSizesHistogram)
  metrics.register(VeProcess.PutThroughputHistogramMetric,  putThroughputsHistogram)
  metrics.register(VeProcess.NumTrackedAllocationsMetric, new Gauge[Long] {
      def getValue: Long = heapRecords.size
    }
  )
  metrics.register(VeProcess.TrackBytesAllocatedMetric, new Gauge[Long] {
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

  logger.info(s"[${handle.address}] Opened VE process (Node ${node}) @ ${handle.address}: ${handle}")
  logger.info(s"[${handle.address}] Opened VEO asynchronous contexts @ ${tcontexts.map(_.address).mkString("[ ", ", ", " ]")}")
  logger.info(s"[${handle.address}] VEO version ${version}; API version ${apiVersion}")

  private[vectorengine] def requireValidBufferForPut(buffer: Pointer): Unit = {
    require(buffer.address > 0L, s"[${handle.address}] Buffer has an invalid address ${buffer.address}; either it is un-initialized or already closed")
    require(buffer.nbytes > 0L, s"[${handle.address}] Buffer has a declared size of ${buffer.nbytes}; nothing to put to VE memory")
  }

  private[vectorengine] def requireValidBufferForGet(buffer: Pointer): Unit = {
    /*
      For `get()`s, there are situations where a 0-sized buffer is fetched,
      (e.g. the output of a filter on a column vector can be column vector of
      size 0), and so we don't require `buffer.nbytes` to be > 0L
    */
    require(buffer.address > 0L, s"[${handle.address}] Buffer has an invalid address ${buffer.address}; either it is un-initialized or already closed")
  }

  private[vectorengine] def withVeoProc[T](thunk: => T): T = {
    openlock.readLock.lock
    try {
      require(opened, "VE process is closed")
      thunk
    } finally {
      openlock.readLock.unlock
    }
  }

  private[vectorengine] def withVeoThread[T](thunk: veo_thr_ctxt => T): T = {
    // Fetch the thread context that is least busy (i.e. has the shortest lock queue)
    val tuple = contextLocks.minBy { case (tlock, _) => tlock.getQueueLength }
    withVeoThreadLock(tuple)(thunk)
  }

  private[vectorengine] def withVeoThread[T](contextId: Long)(thunk: veo_thr_ctxt => T): T = {
    // Fetch the thread context and its lock by the context id
    val tuple = contextLocks.find { case (_, tcontext) => tcontext.address == contextId }.get
    withVeoThreadLock(tuple)(thunk)
  }

  private[vectorengine] def withVeoThreadLock[T](tuple: (ReentrantReadWriteLock, veo_thr_ctxt))
                                                (thunk: veo_thr_ctxt => T): T = {
    withVeoProc {
      // Lock access to the thread context
      val (tlock, tcontext) = tuple
      tlock.writeLock.lock
      try {
        // Perform the async task with the thread context
        thunk(tcontext)
      } finally {
        // Unlock the thread context
        tlock.writeLock.unlock
      }
    }
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

  lazy val numThreads: Int = {
    tcontexts.size
  }

  def cycloneLibrary: Option[LibraryReference] = {
    Option(libCyclone)
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

  private[vectorengine] def _alloc(out: LongPointer, size: Long): Long = {
    require(libCyclone != null, s"[${handle.address}] ${LibCyclone.FileName} has not been loaded yet")

    withVeoProc {
      val func = getSymbol(libCyclone, LibCyclone.AllocFn)
      val args = newArgsStack(Seq(U64Arg(size), BuffArg(VeArgIntent.Out, out)))
      val retp = awaitResult(callAsync(func, args))
      freeArgsStack(args)
      val res = retp.get.toInt
      retp.close
      res
    }
  }

  def allocate(size: Long): VeAllocation = {
    withVeoProc {
      require(size > 0L, s"[${handle.address}] Requested allocation size ${size} is invalid")

      // Value is initialized to 0
      val ptr = new LongPointer(1)
      val (result, duration) = measureTime { _alloc(ptr, size) }

      // Ensure memory is properly allocated
      val address = ptr.get
      require(result == 0, s"[${handle.address}] Memory allocation failed with code: ${result}")
      require(address > 0, s"[${handle.address}] Memory allocation returned an invalid address: ${ptr.get}")
      logger.trace(s"[${handle.address}] Allocated ${size} bytes ==> ${ptr}")
      ptr.close

      // Record metrics
      allocTimer.update(Duration.ofNanos(duration))
      allocSizesHistogram.update(size)

      // Create an allocation record to track the allocation
      val allocation = VeAllocation(address, size, new Exception().getStackTrace)
      heapRecords.put(address, allocation)
      allocation
    }
  }

  def registerAllocation(address: Long, size: Long): VeAllocation = {
    // Explicitly allow address of 0
    require(address >= 0L, s"[${handle.address}] Provided memory address ${address} is invalid; cannot register allocation!")
    // Explicitly allow registrations of zero-sized allocations
    require(size >= 0L, s"[${handle.address}] Provided memory size ${size} is invalid; cannot register allocation!")

    heapRecords.get(address) match {
      case Some(allocation) if allocation.size == size =>
        // Complain only if it's an allocation with address != 0
        if (address > 0) logger.warn(s"[${handle.address}] Allocation for ${size} bytes @ ${address} is already registered")
        allocation

      case Some(allocation) =>
        throw new IllegalArgumentException(s"[${handle.address}] Attempted to register allocation @ ${address} (${size} bytes) but it is already registered with a different size (${allocation.size} bytes)!")

      case None =>
        logger.debug(s"[${handle.address}] Registering externally-created VE memory allocation of ${size} bytes @ ${address}")

        // Record metrics
        allocSizesHistogram.update(size)

        // Register the allocation
        val allocation = VeAllocation(address, size, new Exception().getStackTrace)
        heapRecords.put(address, allocation)
        allocation
    }
  }

  def unregisterAllocation(address: Long): Unit = {
    // Explicitly allow address of 0
    require(address >= 0L, s"[${handle.address}] Provided memory address ${address} is invalid; cannot un-register allocation!")

    heapRecords.get(address) match {
      case Some(allocation) =>
        logger.debug(s"[${handle.address}] Unregistering VE memory allocation tracked by ${getClass.getSimpleName} (${allocation.size} bytes @ ${allocation.address})")
        heapRecords.remove(address)

      case None =>
        logger.warn(s"[${handle.address}] VE memory location @ ${address} is not tracked by ${getClass.getSimpleName}; no allocation to unregister")
    }
  }

  private[vectorengine] def _free(addresses: Seq[Long]): Int = {
    require(libCyclone != null, s"[${handle.address}] ${LibCyclone.FileName} has not been loaded yet")

    withVeoProc {
      val func = getSymbol(libCyclone, LibCyclone.FreeFn)

      // Create array of addresses
      val buffer = addresses.zipWithIndex
        .foldLeft(new LongPointer(addresses.size)) { case (buf, (address, i)) =>
          buf.put(i, address)
        }

      // Create args stack and call free()
      val args = newArgsStack(Seq(
        BuffArg(VeArgIntent.In, buffer),
        U64Arg(addresses.size)
      ))
      val retp = awaitResult(callAsync(func, args))
      val res = retp.get.toInt

      // Cleanup
      freeArgsStack(args)
      retp.close
      buffer.close
      res
    }
  }

  def free(address: Long, unsafe: Boolean): Unit = {
    freeSeq(Seq(address), unsafe)
  }

  def freeSeq(addresses: Seq[Long], unsafe: Boolean): Unit = {
    withVeoProc {
      val toFree = addresses.flatMap { address =>
        // Explicitly allow address of 0
        require(address >= 0L, s"[${handle.address}] Invalid VE memory address provided: ${address}")

        heapRecords.get(address) match {
          case Some(allocation) =>
            logger.debug(s"[${handle.address}] Deallocating pointer @ ${address} (${allocation.size} bytes)")
            Seq(address)

          case None if unsafe =>
            logger.warn(s"[${handle.address}] Releasing VE memory @ ${address} without safety checks!")
            Seq(address)

          case None if address == 0 =>
            // Do nothing for free(0)
            Seq()

          case None =>
            logger.error(s"[${handle.address}] VE memory address does not correspond to a tracked allocation: ${address}; will not call veo_free_mem()")
            Seq()
        }
      }

      /*
        Remove from the records upfront, because a failed free is a full
        crash anyway, and in a highly parallel setup the VE can allocate the
        just-freed memory before we have removed them from the records
      */
      toFree.foreach { address =>
        heapRecords.remove(address)
      }

      val (result, duration) = measureTime { _free(toFree) }
      require(result == 0, s"[${handle.address}] Memory release failed with code: ${result}")
      freeTimer.update(Duration.ofNanos(duration))
    }
  }

  def freeAll: Unit = {
    withVeoProc {
      logger.debug(s"[${handle.address}] Releasing all ${heapRecords.size} heap allocations held by the process")
      freeSeq(heapRecords.keys.toSeq)

      logger.debug(s"[${handle.address}] Releasing all ${heapRecords.size} veo_args allocations held by the process")
      stackRecords.values.foreach(freeArgsStack)
    }
  }

  def put(buffer: Pointer): VeAllocation = {
    withVeoProc {
      // Allocate VE memory
      requireValidBufferForPut(buffer)
      val allocation = allocate(buffer.nbytes)

      val (resultPtr, duration) = measureTime {
        awaitResult(putAsync(buffer, allocation.address))
      }
      val result = resultPtr.get

      require(result == 0, s"await of veo_async_write_mem failed and returned ${result}")

      // Log transfer metrics
      val throughput = (buffer.nbytes / 1024 / 1024) / (duration / 1e9)
      putThroughputsHistogram.update(throughput.toLong)
      putSizesHistogram.update(buffer.nbytes)
      logger.debug(s"[${handle.address}] Transfer of ${buffer.nbytes} bytes to the VE took ${duration / 1e6} ms (${throughput} MB/s)")

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
    withVeoThread { tcontext =>
      requireValidBufferForPut(buffer)
      require(destination > 0L, s"[${handle.address}][${tcontext.address}] Invalid VE memory address for put: ${destination}")
      val id = veo.veo_async_write_mem(tcontext, destination, buffer, buffer.nbytes)
      require(id != veo.VEO_REQUEST_ID_INVALID, s"[${handle.address}][${tcontext.address}] veo_async_write_mem failed and returned ${id}")
      VeAsyncReqId(id, tcontext.address)
    }
  }

  def get(buffer: Pointer, source: Long): Unit = {
    awaitResult(getAsync(buffer, source))
  }

  def getAsync(buffer: Pointer, source: Long): VeAsyncReqId = {
    withVeoThread { tcontext =>
      requireValidBufferForGet(buffer)
      require(source > 0L, s"[${handle.address}][${tcontext.address}] Invalid VE memory address for get: ${source}")
      val id = veo.veo_async_read_mem(tcontext, buffer, source, buffer.nbytes)
      require(id != veo.VEO_REQUEST_ID_INVALID, s"[${handle.address}][${tcontext.address}] veo_async_read_mem failed and returned ${id}")
      VeAsyncReqId(id, tcontext.address)
    }
  }

  def peekResult(id: VeAsyncReqId): (Int, LongPointer) = {
    withVeoThread(id.context) { tcontext =>
      val retp = new LongPointer(1)
      retp.put(Long.MinValue)
      val res = veo.veo_call_peek_result(tcontext, id.value, retp)
      (res, retp)
    }
  }

  def awaitResult(id: VeAsyncReqId): LongPointer = {
    withVeoThread(id.context) { tcontext =>
      val retp = new LongPointer(1)
      retp.put(Long.MinValue)
      val res = veo.veo_call_wait_result(tcontext, id.value, retp)
      require(res == veo.VEO_COMMAND_OK, s"[${handle.address}][${tcontext.address}] VE function returned value: ${res}")
      retp
    }
  }

  private[vectorengine] def _load(path: Path): LibraryReference = {
    withVeoProc {
      loadedLibRecords.synchronized {
        val npath = path.normalize.toAbsolutePath
        loadedLibRecords.get(npath.toString) match {
          case Some(lib) =>
            logger.debug(s"[${handle.address}] Library .SO has already been loaded: ${npath}")
            lib

          case None =>
            // AVEO has a check on paths to be less than 255 chars
            require(npath.toString.size <= veo.VEO_SYMNAME_LEN_MAX, s"[${handle.address}] Path is longer than ${veo.VEO_SYMNAME_LEN_MAX} characters, which AVEO will not load")
            require(Files.exists(npath), s"[${handle.address}] Path does not correspond to an existing file: ${npath}")

            logger.info(s"[${handle.address}] Loading from path as .SO: ${npath}...")
            val result = veo.veo_load_library(handle, npath.toString)
            require(result > 0, s"[${handle.address}] Expected library reference to be > 0, got ${result} (library at: ${npath})")

            // Create a library load record to track
            val lib = LibraryReference(npath.toString, result)
            loadedLibRecords.put(npath.toString, lib)
            lib
        }
      }
    }
  }

  def load(path: Path): LibraryReference = {
    // Always try to load libcyclone.so first
    if (libCyclone == null) {
      val libCyclonePath = if (path.endsWith(LibCyclone.FileName)) {
        path
      } else {
        path.getParent.resolve("sources").resolve(LibCyclone.FileName)
      }
      libCyclone = _load(libCyclonePath)
    }

    _load(path)
  }

  private[vectorengine] def _unload(lib: LibraryReference): Unit = {
    withVeoProc {
      loadedLibRecords.synchronized {
        val npath = Paths.get(lib.path).normalize.toAbsolutePath
        loadedLibRecords.get(npath.toString) match {
          case Some(lib) =>
            logger.info(s"[${handle.address}] Unloading library from the VE process: ${npath}...")
            val result = veo.veo_unload_library(handle, lib.value)
            require(result == 0, s"[${handle.address}] Failed to unload library from the VE process, got ${result} (library at: ${npath})")
            // Remove only after the veo_unload_library() was successful
            loadedLibRecords.remove(npath.toString)

          case None =>
            throw new IllegalArgumentException(s"[${handle.address}] VE process does not have library loaded; nothing to unload: ${npath}")
        }
      }
    }
  }

  def unload(lib: LibraryReference): Unit = {
    _unload(lib)

    // Remove the reference to libcyclone.so if that was the library unloaded
    if (lib.path.endsWith(LibCyclone.FileName)) {
      libCyclone = null
    }
  }

  def getSymbol(lib: LibraryReference, name: String): LibrarySymbol = {
    withVeoProc {
      require(name.trim.nonEmpty, s"[${handle.address}] Symbol name is empty or contains only whitespaces")
      val result = veo.veo_get_sym(handle, lib.value, name)
      require(result > 0, s"[${handle.address}] Expected > 0, but got ${result} when looking up symbol '${name}' (library at: ${lib.path})")
      LibrarySymbol(lib, name, result)
    }
  }

  def newArgsStack(inputs: Seq[CallStackArgument]): VeCallArgsStack = {
    withVeoProc {
      val args = veo.veo_args_alloc
      require(! args.isNull,  s"[${handle.address}] Fail to allocate arguments stack")
      logger.trace(s"[${handle.address}] Allocated veo_args @ ${args.address}")

      inputs.zipWithIndex.foreach {
        case (I32Arg(value), i) =>
          val result = veo.veo_args_set_i32(args, i, value)
          require(result == 0, s"[${handle.address}] Failed to set arguments stack at position ${i} to: ${value}")
          logger.trace(s"[${handle.address}] [veo_args @ ${args.address}] Insert @ position ${i}: ${value}")

        case (U64Arg(value), i) =>
          val result = veo.veo_args_set_u64(args, i, value)
          require(result == 0, s"[${handle.address}] Failed to set arguments stack at position ${i} to: ${value}")
          logger.trace(s"[${handle.address}] [veo_args @ ${args.address}] Insert @ position ${i}: ${value}")

        case (BuffArg(intent, buffer), i) =>
          val icode = intent match {
            case VeArgIntent.In     => veo.VEO_INTENT_IN
            case VeArgIntent.Out    => veo.VEO_INTENT_OUT
            case VeArgIntent.InOut  => veo.VEO_INTENT_INOUT
          }
          val result = veo.veo_args_set_stack(args, icode, i, new BytePointer(buffer), buffer.nbytes)
          require(result == 0, s"[${handle.address}] Failed to set arguments stack at position ${i} to: ${buffer}")
          logger.trace(s"[${handle.address}] [veo_args @ ${args.address}] Insert @ position ${i}: ${buffer.getClass.getSimpleName} buffer @ VH ${buffer.address} (${buffer.nbytes} bytes)")
      }

      // Create an allocation record to track the allocation
      val allocation = VeCallArgsStack(inputs, args)
      stackRecords.synchronized {
        stackRecords.put(args.address, allocation)
      }
      allocation
    }
  }

  def freeArgsStack(stack: VeCallArgsStack): Unit = {
    withVeoProc {
      stackRecords.synchronized {
        stackRecords.get(stack.args.address) match {
          case Some(allocation) =>
            logger.trace(s"[${handle.address}] Releasing veo_args @ ${stack.args.address}")
            veo.veo_args_free(stack.args)
            // Remove only after the free() was successful
            stackRecords.remove(stack.args.address)

          case None =>
            throw new IllegalArgumentException(s"[${handle.address}] VeCallArgsStack does not correspond to a tracked veo_args allocation: ${stack.args.address}")
        }
      }
    }
  }

  def call(func: LibrarySymbol, stack: VeCallArgsStack): LongPointer = {
    withVeoProc {
      val (result, duration) = measureTime { awaitResult(callAsync(func, stack)) }

      // Record metrics
      syncFnCalls += 1
      syncFnCallDurations += duration
      syncFnCallTimer.update(Duration.ofNanos(duration))

      logger.debug(
        s"[${handle.address}] Finished call to '${func.name}': ${syncFnCalls} VeSeconds: (${syncFnCallDurations / 1e9} s)"
      )

      result
    }
  }

  def callAsync(func: LibrarySymbol, stack: VeCallArgsStack): VeAsyncReqId = {
    withVeoThread { tcontext =>
      logger.debug(s"[${handle.address}][${tcontext.address}] Making VE async call '${func.name}' with veo_args @ ${stack.args.address}")

      val id = veo.veo_call_async(tcontext, func.address, stack.args)
      require(
        id != veo.VEO_REQUEST_ID_INVALID,
        s"[${handle.address}][${tcontext.address}] VE async call failed for function '${func.name}' (library at: ${func.lib.path})"
      )

      VeAsyncReqId(id, tcontext.address)
    }
  }

  def close: Unit = {
    openlock.writeLock.lock

    try {
      if (opened) {
        val MaxToShow = 5

        // Complain about un-released context locks
        val cRecords = contextLocks.filter(_._1.isWriteLocked)
        if (cRecords.nonEmpty) {
          logger.error(s"[${handle.address}] There are still locked aynchronous VEO contexts:")
          cRecords.foreach { case (tlock, tcontext) =>
            logger.error(s"[${handle.address}] Context: ${tcontext.address}: Queue Size = ${tlock.getQueueLength}")
          }
        } else {
          logger.info(s"[${handle.address}] There are no locked asynchronous VEO contexts; this is good.")
        }

        // Complain about un-released heap allocations
        val hRecords = heapRecords.take(MaxToShow)
        if (hRecords.nonEmpty) {
          logger.error(s"[${handle.address}] There were ${heapRecords.size} unreleased heap allocations. First ${MaxToShow}:")
          hRecords.foreach { case (_, record) =>
            logger.error(s"[${handle.address}] Position: ${record.address}", record.toThrowable)
          }
        } else {
          logger.info(s"[${handle.address}] There are no unreleased heap allocations; this is good.")
        }

        // Complain about un-released args stack allocations
        val sRecords = stackRecords.take(MaxToShow)
        if (sRecords.nonEmpty) {
          logger.error(s"[${handle.address}] There were ${stackRecords.size} unreleased stack allocations. First ${MaxToShow}:")
          sRecords.foreach { case (_, record) =>
            logger.error(s"[${handle.address}] Position: ${record.args.address}")
          }
        } else {
          logger.info(s"[${handle.address}] There are no unreleased stack allocations; this is good.")
        }

        Try {
          /*
            There is no need to close the VEO asynchronous contexts individually
            since closing the process closes everything.
          */
          logger.info(s"[${handle.address}] Closing VE process (Node ${node}) @ ${handle.address}")
          veo.veo_proc_destroy(handle)

          logger.info(s"[${handle.address}] Clearing allocation records held by the VE process")
          heapRecords.clear

          logger.info(s"[${handle.address}] Clearing veo_args allocations records held by the VE process")
          stackRecords.clear

          logger.info(s"[${handle.address}] Clearing loaded libraries records held by the VE process")
          loadedLibRecords.clear
        }

        opened = false

      } else {
        logger.info(s"[${handle.address}] Process has already been closed!")
      }

    } finally {
      openlock.writeLock.unlock
    }
  }
}
