package com.nec.vectorengine

import com.nec.colvector.{VeColVectorSource => VeSource}
import scala.util.Try
import java.nio.file.Path
import org.apache.spark.api.plugin.PluginContext
import org.bytedeco.javacpp.{BytePointer, LongPointer, Pointer}
import org.bytedeco.veoffload.global.veo
import org.bytedeco.veoffload.{veo_args, veo_proc_handle, veo_thr_ctxt}
import com.typesafe.scalalogging.LazyLogging

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

  def allocate(size: Long): Long

  def free(location: Long): Unit

  def put(buffer: BytePointer): Long

  def putAsync(buffer: BytePointer): (Long, VeAsyncReqId)

  def putAsync(buffer: BytePointer, destination: Long): VeAsyncReqId

  def get(source: Long, size: Long): BytePointer

  def getAsync(buffer: BytePointer, source: Long): VeAsyncReqId

  /*
    NOTE: `peek` here means "pick up a result from VE function if it has
    finished", i.e. `peek and get`
  */
  def peekResult(id: VeAsyncReqId): (Int, Long)

  def awaitResult(id: VeAsyncReqId): Long

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
    /*
      If venode is -1, a VE process is created on the VE node specified by
      environment variable VE_NODE_NUMBER. If venode is -1 and environment
      variable VE_NODE_NUMBER is not set, a VE process is created on the VE
      node #0.
     */
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

  def create(venode: Int, identifier: String): VeProcess = {
    createVeoTuple(venode) match {
      case Some((venode, handle, tcontext)) =>
        WrappingVeo(venode, identifier, handle, tcontext)

      case None =>
        throw new IllegalArgumentException(s"VE process could not be allocated for node ${venode}; either the node is offline or another VE process is running")
    }
  }

  // def createFromContext(context: PluginContext): VeProcess = {
  //   val resources = context.resources
  //   logger.info(s"Executor has the following resources available => ${resources}")

  //   val selectedNodeId = if (!resources.containsKey("ve")) {
  //     val id = Try { System.getenv("VE_NODE_NUMBER").toInt }.getOrElse(DefaultVeNodeId)
  //     logger.info(s"VE resources are not available from the PluginContext; will use '${id}' as the main resource.")
  //     id

  //   } else {
  //     val veResources = resources.get("ve")

  //     // Executor IDs start at 1
  //     val executorId = Try { context.executorID.toInt - 1 }.getOrElse(0)
  //     val veMultiple = executorId / MaxVeNodes

  //     if (veMultiple > veResources.addresses.size) {
  //       logger.warn("Not enough VE resources allocated for the number of executors specified.")
  //     }

  //     veResources.addresses(veMultiple % veResources.addresses.size).toInt
  //   }

  //   logger.info(s"Attemping to use VE node = ${selectedNodeId}")

  //   var currentNodeId = selectedNodeId
  //   var handle: veo_proc_handle = null
  //   var tcontext: veo_thr_ctxt = null

  //   while (handle == null && currentNodeId < MaxVeNodes) {
  //     handle = veo.veo_proc_create(currentNodeId)
  //     tcontext = veo.veo_context_open(handle)

  //     if (handle == null) {
  //       logger.warn(s"VE process could not be allocated for node ${currentNodeId}, trying next.")
  //       currentNodeId += 1
  //     }
  //   }

  //   require(
  //     handle != null && handle.address > 0,
  //     s"VE process could not be allocated; all nodes are either offline or occupied by another VE process"
  //   )

  //   require(
  //     tcontext != null && tcontext.address > 0,
  //     s"VEO asynchronous context could not be allocated for node ${currentNodeId}"
  //   )

  //   val identifier = s"VE Process @ ${handle.address}, Executor ${Try { context.executorID }}"
  //   WrappingVeo(currentNodeId, identifier, handle, tcontext)
  // }

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
