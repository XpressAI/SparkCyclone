package com.nec.vectorengine

import com.nec.colvector.{VeColVectorSource => VeSource}
import java.nio.file.Path
import com.typesafe.scalalogging.LazyLogging
import org.bytedeco.javacpp.{BytePointer, LongPointer, Pointer}
import org.bytedeco.veoffload.veo_proc_handle

final case class DeferredVeProcess(newproc: () => VeProcess) extends VeProcess with LazyLogging {
  private lazy val underlying = newproc()

  def node: Int = {
    underlying.node
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

  def allocate(size: Long): Long = {
    underlying.allocate(size)
  }

  def free(location: Long): Unit = {
    underlying.free(location)
  }

  def put(buffer: Pointer): Long = {
    underlying.put(buffer)
  }

  def putAsync(buffer: Pointer): (Long, VeAsyncReqId) = {
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
    underlying.unload(lib)
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
    underlying.close
  }
}
