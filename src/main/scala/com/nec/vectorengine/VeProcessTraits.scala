package com.nec.vectorengine

import com.nec.colvector.{VeColVectorSource => VeSource}
import scala.util.Try
import java.nio.file.Path
import org.apache.spark.api.plugin.PluginContext
import org.bytedeco.javacpp.{LongPointer, Pointer}
import org.bytedeco.veoffload.global.veo
import org.bytedeco.veoffload.{veo_args, veo_proc_handle, veo_thr_ctxt}
import com.typesafe.scalalogging.LazyLogging

final case class VeAllocation private[vectorengine] (address: Long, size: Long, trace: Seq[StackTraceElement]) {
  def toThrowable: Throwable = {
    new Throwable {
      override def getMessage: String = {
        s"Unreleased allocation @ ${address} (${size} bytes)"
      }

      override def getStackTrace: Array[StackTraceElement] = {
        trace.toArray
      }
    }
  }
}

final case class LibraryReference private[vectorengine] (path: Path, value: Long)

final case class LibrarySymbol private[vectorengine] (lib: LibraryReference, name: String, address: Long)

final case class VeCallArgsStack private[vectorengine] (inputs: Seq[CallStackArgument], args: veo_args)

sealed trait VeArgIntent

object VeArgIntent {
  final case object In extends VeArgIntent
  final case object Out extends VeArgIntent
  final case object InOut extends VeArgIntent
}

sealed trait CallStackArgument

final case class I32Arg(value: Int) extends CallStackArgument

final case class U64Arg(value: Long) extends CallStackArgument

final case class BuffArg(intent: VeArgIntent, buffer: Pointer) extends CallStackArgument

final case class VeAsyncReqId private[vectorengine] (value: Long)

trait VeProcess {
  def node: Int

  def source: VeSource

  def isOpen: Boolean

  def apiVersion: Int

  def version: String

  def heapAllocations: Map[Long, VeAllocation]

  def stackAllocations: Map[Long, VeCallArgsStack]

  def allocate(size: Long): VeAllocation

  def free(address: Long): Unit

  def freeAll: Unit

  def put(buffer: Pointer): VeAllocation

  def putAsync(buffer: Pointer): (VeAllocation, VeAsyncReqId)

  def putAsync(buffer: Pointer, destination: Long): VeAsyncReqId

  def get(buffer: Pointer, source: Long): Unit

  def getAsync(buffer: Pointer, source: Long): VeAsyncReqId

  /*
    NOTE: `peek` here means "pick up a result from VE function if it has
    finished", i.e. `peek and get`
  */
  def peekResult(id: VeAsyncReqId): (Int, LongPointer)

  def awaitResult(id: VeAsyncReqId): LongPointer

  def load(path: Path): LibraryReference

  def unload(lib: LibraryReference): Unit

  def getSymbol(lib: LibraryReference, symbol: String): LibrarySymbol

  def newArgsStack(inputs: Seq[CallStackArgument]): VeCallArgsStack

  def freeArgsStack(stack: VeCallArgsStack): Unit

  def call(func: LibrarySymbol, stack: VeCallArgsStack): LongPointer

  def callAsync(func: LibrarySymbol, stack: VeCallArgsStack): VeAsyncReqId

  def close: Unit
}

object VeProcess extends LazyLogging {
  final val DefaultVeNodeId = 0
  final val MaxVeNodes = 8

  private def createVeoTuple(venode: Int): Option[(Int, veo_proc_handle, veo_thr_ctxt)] = {
    val nnum = if (venode < -1) venode.abs else venode
    logger.info(s"Attemping to allocate VE process on node ${nnum}...")

    for {
      // Create the process handle
      handle <- {
        val h = veo.veo_proc_create(nnum)
        if (h != null && h.address > 0) Some(h) else None
      }
      _ = logger.info(s"Successfully allocated VE process on node ${nnum}")

      // Create asynchronous context
      tcontext <- {
        val t = veo.veo_context_open(handle)
        if (t != null && t.address > 0) Some(t) else None
      }
      _ = logger.info(s"Successfully allocated VEO asynchronous context on node ${nnum}")

    } yield {
      // Return the tuple
      (nnum, handle, tcontext)
    }
  }

  def create(identifier: String): VeProcess = {
    val tupleO = 0.until(MaxVeNodes).foldLeft(Option.empty[(Int, veo_proc_handle, veo_thr_ctxt)]) {
      case (Some(tuple), venode)  => Some(tuple)
      case (None, venode)         => createVeoTuple(venode)
    }

    tupleO match {
      case Some((venode, handle, tcontext)) =>
        WrappingVeo(venode, identifier, handle, tcontext)

      case None =>
        throw new IllegalArgumentException(s"VE process could not be allocated; all nodes are either offline or occupied by another VE process")
    }
  }

  /*
    If venode is -1, a VE process is created on the VE node specified by
    environment variable VE_NODE_NUMBER. If venode is -1 and environment
    variable VE_NODE_NUMBER is not set, a VE process is created on the VE
    node #0.
  */
  def create(venode: Int, identifier: String): VeProcess = {
    createVeoTuple(venode) match {
      case Some((venode, handle, tcontext)) =>
        WrappingVeo(venode, identifier, handle, tcontext)

      case None =>
        throw new IllegalArgumentException(s"VE process could not be allocated for node ${venode}; either the node is offline or another VE process is running")
    }
  }

  def createFromContext(context: PluginContext): VeProcess = {
    val resources = context.resources
    logger.info(s"Executor has the following resources available => ${resources}")

    val selectedNodeId = if (!resources.containsKey("ve")) {
      val id = Try { System.getenv("VE_NODE_NUMBER").toInt }.getOrElse(DefaultVeNodeId)
      logger.info(s"VE resources are not available from the PluginContext; will use '${id}' as the main resource.")
      id

    } else {
      val veResources = resources.get("ve")

      // Executor IDs start at 1
      val executorId = Try { context.executorID.toInt - 1 }.getOrElse(0)
      val veMultiple = executorId / MaxVeNodes

      if (veMultiple > veResources.addresses.size) {
        logger.warn("Not enough VE resources allocated for the number of executors specified.")
      }

      veResources.addresses(veMultiple % veResources.addresses.size).toInt
    }

    logger.info(s"Attemping to use VE node = ${selectedNodeId}")

    val tupleO = selectedNodeId.until(MaxVeNodes).foldLeft(Option.empty[(Int, veo_proc_handle, veo_thr_ctxt)]) {
      case (Some(tuple), venode)  => Some(tuple)
      case (None, venode)         => createVeoTuple(venode)
    }

    tupleO match {
      case Some((venode, handle, tcontext)) =>
        val identifier = s"VE Process @ ${handle.address}, Executor ${Try { context.executorID }}"
        WrappingVeo(venode, identifier, handle, tcontext)

      case None =>
        throw new IllegalArgumentException(s"VE process could not be allocated; all nodes are either offline or occupied by another VE process")
    }
  }
}
