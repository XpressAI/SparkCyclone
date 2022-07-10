package io.sparkcyclone.vectorengine

import io.sparkcyclone.data.{VeColVectorSource => VeSource}
import java.nio.file.Path
import com.codahale.metrics.MetricRegistry
import com.typesafe.scalalogging.LazyLogging
import org.bytedeco.javacpp.{LongPointer, Pointer}

final case class DeferredVeProcess(newproc: () => VeProcess) extends VeProcess with LazyLogging {
  private var instantiated = false

  private lazy val underlying = synchronized {
    logger.info("Creating the underlying VeProcess")
    val proc = newproc()
    logger.info(s"New underlying VeProcess created: ${proc.source}")
    instantiated = true
    proc
  }

  def node: Int = {
    underlying.node
  }

  def metrics: MetricRegistry = {
    underlying.metrics
  }

  def source: VeSource = {
    underlying.source
  }

  def isOpen: Boolean = {
    underlying.isOpen
  }

  lazy val apiVersion: Int = {
    underlying.apiVersion
  }

  lazy val version: String = {
    underlying.version
  }

  def numThreads: Int = {
    if (instantiated) underlying.numThreads else 0
  }

  def cycloneLibrary: Option[LibraryReference] = {
    if (instantiated) underlying.cycloneLibrary else None
  }

  def heapAllocations: Map[Long, VeAllocation] = {
    underlying.heapAllocations
  }

  def stackAllocations: Map[Long, VeCallArgsStack] = {
    underlying.stackAllocations
  }

  def loadedLibraries: Map[String, LibraryReference] = {
    underlying.loadedLibraries
  }

  def allocate(size: Long): VeAllocation = {
    underlying.allocate(size)
  }

  def registerAllocation(address: Long, size: Long): VeAllocation = {
    underlying.registerAllocation(address, size)
  }

  def unregisterAllocation(address: Long): Unit = {
    underlying.unregisterAllocation(address)
  }

  def free(address: Long, unsafe: Boolean): Unit = {
    // If the VeProcess is not instantiated yet, skip
    if (instantiated) {
      underlying.free(address, unsafe)
    }
  }

  def freeSeq(addresses: Seq[Long], unsafe: Boolean = false): Unit = {
    // If the VeProcess is not instantiated yet, skip
    if (instantiated) {
      underlying.freeSeq(addresses, unsafe)
    }
  }

  def freeAll: Unit = {
    // If the VeProcess is not instantiated yet, skip
    if (instantiated) {
      underlying.freeAll
    }
  }

  def put(buffer: Pointer): VeAllocation = {
    underlying.put(buffer)
  }

  def putAsync(buffer: Pointer): (VeAllocation, VeAsyncReqId) = {
    underlying.putAsync(buffer)
  }

  def putAsync(buffer: Pointer, destination: Long): VeAsyncReqId = {
    underlying.putAsync(buffer, destination)
  }

  def get(buffer: Pointer, source: Long): Unit = {
    underlying.get(buffer, source)
  }

  def getAsync(buffer: Pointer, source: Long): VeAsyncReqId = {
    underlying.getAsync(buffer, source)
  }

  def peekResult(id: VeAsyncReqId): (Int, LongPointer) = {
    underlying.peekResult(id)
  }

  def awaitResult(id: VeAsyncReqId): LongPointer = {
    underlying.awaitResult(id)
  }

  def load(path: Path): LibraryReference = {
    underlying.load(path)
  }

  def unload(lib: LibraryReference): Unit = {
    // If the VeProcess is not instantiated yet, skip
    if (instantiated) {
      underlying.unload(lib)
    }
  }

  def getSymbol(lib: LibraryReference, symbol: String): LibrarySymbol = {
    underlying.getSymbol(lib, symbol)
  }

  def newArgsStack(inputs: Seq[CallStackArgument]): VeCallArgsStack = {
    underlying.newArgsStack(inputs)
  }

  def freeArgsStack(stack: VeCallArgsStack): Unit = {
    underlying.freeArgsStack(stack)
  }

  def call(func: LibrarySymbol, stack: VeCallArgsStack): LongPointer = {
    underlying.call(func, stack)
  }

  def callAsync(func: LibrarySymbol, stack: VeCallArgsStack): VeAsyncReqId = {
    underlying.callAsync(func, stack)
  }

  def close: Unit = {
    // If the VeProcess is not instantiated yet, skip
    if (instantiated) {
      underlying.close
    }
  }
}
