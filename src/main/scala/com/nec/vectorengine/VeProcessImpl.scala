package com.nec.vectorengine

import com.nec.colvector.{VeColVectorSource => VeSource}
import scala.util.Try
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import com.typesafe.scalalogging.LazyLogging
import org.bytedeco.javacpp.{BytePointer, LongPointer}
import org.bytedeco.veoffload.global.veo
import org.bytedeco.veoffload.{veo_args, veo_proc_handle, veo_thr_ctxt}

final case class WrappingVeo private (val node: Int,
                                      identifier: String,
                                      handle: veo_proc_handle,
                                      tcontext: veo_thr_ctxt)
                                      extends VeProcess with LazyLogging {
  logger.info(s"Opened VE process (Node ${node}) @ ${handle.address}: ${handle}")
  logger.info(s"Opened VEO asynchronous context @ ${tcontext.address}: ${tcontext}")
  logger.info(s"VEO version: ${version}; API version ${apiVersion}")

  private var opened = true

  private[vectorengine] def requireValidBuffer(buffer: BytePointer): Unit = {
    require(buffer.address > 0, s"Buffer has an invalid address ${buffer.address}; either it is un-initialized or already closed")
    require(buffer.limit() > 0, s"Buffer has a declared size of ${buffer.limit()}")
  }

  def isOpen: Boolean = {
    opened
  }

  lazy val apiVersion: Int = {
    veo.veo_api_version
  }

  lazy val version: String = {
    val bytes = veo.veo_version_string
    bytes.getString(StandardCharsets.US_ASCII)
  }

  lazy val source: VeSource = {
    VeSource(identifier)
  }

  def allocate(size: Long): Long = {
    require(size > 0L, s"Requested size ${size} is invalid")
    val ptr = new LongPointer(1)
    val result = veo.veo_alloc_mem(handle, ptr, size)
    val location = ptr.get
    require(result == 0, s"Memory allocation failed with code: ${result}")
    require(location > 0, s"Memory allocation returned an invalid address: ${ptr.get}")
    logger.trace(s"Allocated ${size} bytes ==> ${ptr}")
    ptr.close
    location
  }

  def free(location: Long): Unit = {
    require(location > 0L, s"Invalid VE memory address ${location}")
    logger.trace(s"Deallocating pointer @ ${location}")
    val result = veo.veo_free_mem(handle, location)
    require(result == 0, s"Memory release failed with code: ${result}")
  }

  def put(buffer: BytePointer): Long = {
    requireValidBuffer(buffer)
    val location = allocate(buffer.limit())
    val result = veo.veo_write_mem(handle, location, buffer, buffer.limit())
    require(result == 0, s"veo_write_mem failed and returned ${result}")
    location
  }

  def putAsync(buffer: BytePointer): Long = {
    requireValidBuffer(buffer)
    val location = allocate(buffer.limit())
    putAsync(buffer, location)
  }

  def putAsync(buffer: BytePointer, destination: Long): Long = {
    requireValidBuffer(buffer)
    require(destination > 0L, s"Invalid VE memory address ${destination}")
    val id = veo.veo_async_write_mem(tcontext, destination, buffer, buffer.limit())
    require(id != veo.VEO_REQUEST_ID_INVALID, s"veo_async_write_mem failed and returned ${id}")
    id
  }

  def get(source: Long, size: Long): BytePointer = {
    require(source > 0L, s"Invalid VE memory address ${source}")
    require(size > 0L, s"Requested size ${size} is invalid")
    val buffer = new BytePointer(size)
    val result = veo.veo_read_mem(handle, buffer, source, size)
    require(result == 0, s"veo_read_mem failed and returned ${result}")
    buffer
  }

  def getAsync(buffer: BytePointer, source: Long): Long = {
    requireValidBuffer(buffer)
    require(source > 0L, s"Invalid VE memory address ${source}")
    val id = veo.veo_async_read_mem(tcontext, buffer, source, buffer.limit())
    require(id != veo.VEO_REQUEST_ID_INVALID, s"veo_async_read_mem failed and returned ${id}")
    id
  }

  def peekResult(requestId: Long): (Int, Long) = {
    // Pre-initialize value to 0
    val retp = new LongPointer(0)
    val res = veo.veo_call_peek_result(tcontext, requestId, retp)
    val retval = retp.get
    require(retval >= 0L, s"Result should be >= 0, got ${retval}")
    retp.close
    (res, retval)
  }

  def awaitResult(requestId: Long): Long = {
    // Pre-initialize value to 0
    val retp = new LongPointer(0)
    val res = veo.veo_call_wait_result(tcontext, requestId, retp)
    val retval = retp.get
    require(res == veo.VEO_COMMAND_OK, s"VE function returned value: ${res}")
    require(retval >= 0L, s"Result should be >= 0, got ${retval}")
    retp.close
    retval
  }

  def load(path: Path): LibraryReference = {
    require(Files.exists(path), s"Path does not correspond to an existing file: ${path}")
    logger.info(s"Loading from path as .SO: ${path}...")
    val result = veo.veo_load_library(handle, path.toString)
    require(result > 0, s"Expected library reference to be > 0, got ${result} (library at: ${path})")
    LibraryReference(path, result)
  }

  def unload(lib: LibraryReference): Unit = {
    val result = veo.veo_unload_library(handle, lib.value)
    require(result == 0, s"Failed to unload library reference, got ${result} (library at: ${lib.path})")
  }

  def getSymbol(lib: LibraryReference, name: String): LibrarySymbol = {
    val result = veo.veo_get_sym(handle, lib.value, name)
    require(result > 0, s"Expected > 0, but got ${result} when looking up symbol '${name}' (library at: ${lib.path})")
    LibrarySymbol(lib, name, result)
  }

  def newArgsStack(arguments: Seq[CallStackArgument]): VeCallArgsStack = {
    val stack = veo.veo_args_alloc
    require(! stack.isNull,  s"Fail to allocate arguments stack")

    arguments.zipWithIndex.foreach {
      case (I32Arg(value), i) =>
        val result = veo.veo_args_set_i32(stack, i, value)
        require(result == 0, s"Failed to set arguments stack at position ${i} to: ${value}")

      case (U64Arg(value), i) =>
        val result = veo.veo_args_set_u64(stack, i, value)
        require(result == 0, s"Failed to set arguments stack at position ${i} to: ${value}")

      case (BuffArg(intent, buffer, size), i) =>
        val result = veo.veo_args_set_stack(stack, intent, i, buffer, size)
        require(result == 0, s"Failed to set arguments stack at position ${i} to: ${buffer}")
    }

    VeCallArgsStack(stack)
  }

  def freeArgsStack(stack: VeCallArgsStack): Unit = {
    veo.veo_args_free(stack.args)
  }

  def call(func: LibrarySymbol, stack: VeCallArgsStack): LongPointer = {
    // Initialize to 1.  All cyclone functions should return 0 on successful completion
    val fnOutput = new LongPointer(1)
    val callResult = veo.veo_call_sync(handle, func.address, stack.args, fnOutput)

    // The VE call to the function should succeed
    require(
      callResult == 0,
      s"VE call failed for function '${func.name}' (library at: ${func.lib.path}); got ${callResult}"
    )

    // The function itself should return 0
    require(
      fnOutput.get() == 0L,
      s"Expected 0 from function execution, got ${fnOutput.get()} instead."
    )

    fnOutput
  }

  def callAsync(func: LibrarySymbol, stack: VeCallArgsStack): Long = {
    val id = veo.veo_call_async(tcontext, func.address, stack.args)
    require(
      id != veo.VEO_REQUEST_ID_INVALID,
      s"VE async call failed for function '${func.name}' (library at: ${func.lib.path})"
    )
    id
  }

  def close: Unit = {
    if (opened) {
      Try {
        logger.info(s"Closing VE process (Node ${node}) @ ${handle.address}: ${handle}")
        veo.veo_context_close(tcontext)
        veo.veo_proc_destroy(handle)
      }
      opened = false
    }
  }
}
