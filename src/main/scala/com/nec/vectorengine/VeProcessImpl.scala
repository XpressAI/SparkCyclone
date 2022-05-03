package com.nec.vectorengine

import com.nec.colvector.{VeColVectorSource => VeSource}
import scala.util.Try
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import com.typesafe.scalalogging.LazyLogging
import org.bytedeco.javacpp.{BytePointer, LongPointer, Pointer}
import org.bytedeco.veoffload.global.veo
import org.bytedeco.veoffload.{veo_args, veo_proc_handle, veo_thr_ctxt}

final case class WrappingVeo private (val node: Int,
                                      identifier: String,
                                      handle: veo_proc_handle,
                                      tcontext: veo_thr_ctxt)
                                      extends VeProcess with LazyLogging {

  implicit class ExtendedPointer(buffer: Pointer) {
    def nbytes: Long = {
      buffer.limit * buffer.sizeof
    }
  }

  logger.info(s"Opened VE process (Node ${node}) @ ${handle.address}: ${handle}")
  logger.info(s"Opened VEO asynchronous context @ ${tcontext.address}: ${tcontext}")
  logger.info(s"VEO version ${version}; API version ${apiVersion}")

  private var opened = true

  private[vectorengine] def requireValidBuffer(buffer: Pointer): Unit = {
    require(buffer.address > 0, s"Buffer has an invalid address ${buffer.address}; either it is un-initialized or already closed")
    require(buffer.nbytes > 0, s"Buffer has a declared size of ${buffer.nbytes}")
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
    // Value is initialized to 0
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

  def put(buffer: Pointer): Long = {
    requireValidBuffer(buffer)
    val location = allocate(buffer.nbytes)
    val result = veo.veo_write_mem(handle, location, buffer, buffer.nbytes)
    require(result == 0, s"veo_write_mem failed and returned ${result}")
    location
  }

  def putAsync(buffer: Pointer): (Long, VeAsyncReqId) = {
    requireValidBuffer(buffer)
    val location = allocate(buffer.nbytes)
    (location, putAsync(buffer, location))
  }

  def putAsync(buffer: Pointer, destination: Long): VeAsyncReqId = {
    requireValidBuffer(buffer)
    require(destination > 0L, s"Invalid VE memory address ${destination}")
    val id = veo.veo_async_write_mem(tcontext, destination, buffer, buffer.nbytes)
    require(id != veo.VEO_REQUEST_ID_INVALID, s"veo_async_write_mem failed and returned ${id}")
    VeAsyncReqId(id)
  }

  def get(buffer: Pointer, source: Long): Unit = {
    requireValidBuffer(buffer)
    require(source > 0L, s"Invalid VE memory address ${source}")
    val result = veo.veo_read_mem(handle, buffer, source, buffer.nbytes)
    require(result == 0, s"veo_read_mem failed and returned ${result}")
  }

  def getAsync(buffer: Pointer, source: Long): VeAsyncReqId = {
    requireValidBuffer(buffer)
    require(source > 0L, s"Invalid VE memory address ${source}")
    val id = veo.veo_async_read_mem(tcontext, buffer, source, buffer.nbytes)
    require(id != veo.VEO_REQUEST_ID_INVALID, s"veo_async_read_mem failed and returned ${id}")
    VeAsyncReqId(id)
  }

  def peekResult(id: VeAsyncReqId): (Int, LongPointer) = {
    val retp = new LongPointer(1)
    retp.put(Long.MinValue)
    val res = veo.veo_call_peek_result(tcontext, id.value, retp)
    (res, retp)
  }

  def awaitResult(id: VeAsyncReqId): LongPointer = {
    val retp = new LongPointer(1)
    retp.put(Long.MinValue)
    val res = veo.veo_call_wait_result(tcontext, id.value, retp)
    require(res == veo.VEO_COMMAND_OK, s"VE function returned value: ${res}")
    retp
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
    require(name.trim.nonEmpty, "Symbol name is empty or contains only whitespaces")
    val result = veo.veo_get_sym(handle, lib.value, name)
    require(result > 0, s"Expected > 0, but got ${result} when looking up symbol '${name}' (library at: ${lib.path})")
    LibrarySymbol(lib, name, result)
  }

  def newArgsStack(inputs: Seq[CallStackArgument]): VeCallArgsStack = {
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

    VeCallArgsStack(inputs, args)
  }

  def freeArgsStack(stack: VeCallArgsStack): Unit = {
    logger.trace(s"Releasing veo_args @ ${stack.args.address}")
    veo.veo_args_free(stack.args)
  }

  def call(func: LibrarySymbol, stack: VeCallArgsStack): LongPointer = {
    logger.trace(s"Sync call '${func.name}' with veo_args @ ${stack.args.address}; argument values: ${stack.inputs}")

    // Set the output buffer
    val retp = new LongPointer(1)

    // Call the function
    val callResult = veo.veo_call_sync(handle, func.address, stack.args, retp)

    // The VE call to the function should succeed
    require(
      callResult == 0,
      s"VE call failed for function '${func.name}' (library at: ${func.lib.path}); got ${callResult}"
    )

    retp
  }

  def callAsync(func: LibrarySymbol, stack: VeCallArgsStack): VeAsyncReqId = {
    logger.trace(s"Async call '${func.name}' with veo_args @ ${stack.args.address}")

    val id = veo.veo_call_async(tcontext, func.address, stack.args)
    require(
      id != veo.VEO_REQUEST_ID_INVALID,
      s"VE async call failed for function '${func.name}' (library at: ${func.lib.path})"
    )

    VeAsyncReqId(id)
  }

  def close: Unit = {
    if (opened) {
      Try {
        logger.info(s"Closing VEO asynchronous context @ ${tcontext.address}")
        veo.veo_context_close(tcontext)

        logger.info(s"Closing VE process (Node ${node}) @ ${handle.address}")
        veo.veo_proc_destroy(handle)
      }
      opened = false
    }
  }
}
