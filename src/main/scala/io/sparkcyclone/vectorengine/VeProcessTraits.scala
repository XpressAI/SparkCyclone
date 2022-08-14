package io.sparkcyclone.vectorengine

import io.sparkcyclone.data.{VeColVectorSource => VeSource}
import scala.util.Try
import java.nio.file.Path
import com.codahale.metrics.MetricRegistry
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.api.plugin.PluginContext
import org.bytedeco.javacpp.{LongPointer, Pointer}
import org.bytedeco.veoffload.global.veo
import org.bytedeco.veoffload.{veo_args, veo_proc_handle, veo_thr_ctxt}

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

// Keep path field as a String so that LibraryReference can be serialized across RDD maps
final case class LibraryReference private[vectorengine] (path: String, value: Long)

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

final case class VeAsyncReqId private[vectorengine] (value: Long, funcName: String, timestamp: Long, context: Long)

trait VeProcess {
  def node: Int

  def metrics: MetricRegistry

  def source: VeSource

  def isOpen: Boolean

  def apiVersion: Int

  def version: String

  def numThreads: Int

  def cycloneLibrary: Option[LibraryReference]

  def heapAllocations: Map[Long, VeAllocation]

  def stackAllocations: Map[Long, VeCallArgsStack]

  def loadedLibraries: Map[String, LibraryReference]

  /*
    VE memory that is allocated from the VE side through function calls will need
    to be manually registered for allocation tracking, since they are not
    allocated through `veo_alloc_mem`.
  */
  def registerAllocation(address: Long, size: Long): VeAllocation

  def unregisterAllocation(address: Long): Unit

  def allocate(size: Long): VeAllocation

  /*
    If `unsafe` is set to true, then the method will attempt to free the memory
    location even if it's not recorded in the VeProcess' internal allocations
    tracker.  This option is made available only for the purpose of releasing
    memory allocated from the VE side inside function calls (i.e. not allocated
    from the VH side through the `VeProcess` abstraction).
  */
  def free(address: Long, unsafe: Boolean = false): Unit

  def freeSeq(addresses: Seq[Long], unsafe: Boolean = false): Unit

  def freeAll: Unit

  def put(buffer: Pointer): VeAllocation

  def putAsync(buffer: Pointer): (VeAllocation, VeAsyncReqId)

  def putAsync(buffer: Pointer, destination: Long): VeAsyncReqId

  def get(buffer: Pointer, source: Long): Unit

  def getAsync(buffer: Pointer, source: Long): VeAsyncReqId

  /*
    NOTE: `peek` here means "pick up a result from VE function if it has
    finished", i.e. `peek and get` as opposed to `just peek`
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
  final val MaxVeCores = 8

  // Gauges
  final val NumTrackedAllocationsMetric   = "ve.gauges.allocations.count"
  final val TrackBytesAllocatedMetric     = "ve.gauges.allocations.bytes"
  final val VeSyncFnCallsCountMetric      = "ve.gauges.calls.sync.count"
  final val VeSyncFnCallTimesMetric       = "ve.gauges.calls.sync.time"
  // Timers
  final val VeSyncFnCallTimerMetric       = "ve.timers.calls.sync"
  final val VeAllocTimerMetric            = "ve.timers.alloc"
  final val VeFreeTimerMetric             = "ve.timers.free"
  // Histograms
  final val AllocSizesHistogramMetric     = "ve.histograms.allocations.sizes"
  final val PutSizesHistogramMetric       = "ve.histograms.put.size"
  final val PutThroughputHistogramMetric  = "ve.histograms.put.throughput"

  private[vectorengine] def createVeoTuple(venode: Int,
                                           veCores: Int): Option[(Int, veo_proc_handle, Seq[veo_thr_ctxt])] = {
    require(veCores > 0, "veCores must be > 0")
    require(veCores <= MaxVeCores, s"veCores must be <= ${MaxVeCores}")
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
      tcontexts <- Some {
        (0 until veCores).map { i =>
          /*
            NOTE: The first thread context for a process handle is created almost
            immediately, while subsequent thread contexts usually take roughly
            5s to create.
          */
          val tc = veo.veo_context_open(handle)

          /*
            Wait after creating each asynchronous context or else we will
            encounter the following error when subsequently creating the next
            context:

            [VH] [TID 190048] ERROR: veo_context_open() failed to open context: ProcHandle: timeout while waiting for VE.
          */
          Thread.sleep(500)

          if (tc != null && tc.address > 0) {
            logger.info(s"Successfully allocated VEO asynchronous context ${i} on node ${nnum}")
            Some(tc)

          } else {
            logger.error(s"Could not allocate VEO asynchronous context ${i} on node ${nnum}")
            None
          }
        }
        .flatten
      }

    } yield {
      (nnum, handle, tcontexts)
    }
  }

  def create(identifier: String, veCores: Int): VeProcess = {
    create(identifier, veCores, new MetricRegistry)
  }

  def create(identifier: String, veCores: Int, metrics: MetricRegistry): VeProcess = {
    val tupleO = 0.until(MaxVeNodes).foldLeft(Option.empty[(Int, veo_proc_handle, Seq[veo_thr_ctxt])]) {
      case (Some(tuple), venode)  => Some(tuple)
      case (None, venode)         => createVeoTuple(venode, veCores)
    }

    tupleO match {
      case Some((venode, handle, tcontext)) =>
        WrappingVeo(venode, identifier, handle, tcontext, metrics)

      case None =>
        throw new IllegalArgumentException(s"VE process could not be allocated; all nodes are either offline or occupied by another VE process")
    }
  }

  def create(venode: Int, identifier: String, veCores: Int = MaxVeCores): VeProcess = {
    create(venode, identifier, veCores, new MetricRegistry)
  }

  /*
    If venode is -1, a VE process is created on the VE node specified by
    environment variable VE_NODE_NUMBER. If venode is -1 and environment
    variable VE_NODE_NUMBER is not set, a VE process is created on the VE
    node #0.
  */
  def create(venode: Int, identifier: String, veCores: Int, metrics: MetricRegistry): VeProcess = {
    createVeoTuple(venode, veCores) match {
      case Some((venode, handle, tcontexts)) =>
        WrappingVeo(venode, identifier, handle, tcontexts, metrics)

      case None =>
        throw new IllegalArgumentException(s"VE process could not be allocated for node ${venode}; either the node is offline or another VE process is running")
    }
  }

  def createFromContext(context: PluginContext): VeProcess = {
    val resources = context.resources
    logger.info(s"Executor has the following resources available => ${resources}")

    val veCores = Try { context.conf().get("spark.cyclone.resource.ve.cores").toInt }.getOrElse(MaxVeCores)
    logger.info(s"Specified max cores per VE process => ${veCores}")

    val selectedNodeId = if (!resources.containsKey("ve")) {
      val id = Try { System.getenv("VE_NODE_NUMBER").toInt }.getOrElse(DefaultVeNodeId)
      logger.info(s"VE resources are not available from the PluginContext; will use '${id}' as the main resource.")
      id

    } else {
      val veResources = resources.get("ve")

      // Executor IDs start at 1
      val executorId = Try { context.executorID.toInt - 1 }.getOrElse(0)
      val veMultiple = executorId / (MaxVeNodes / veCores)

      if (veMultiple > veResources.addresses.size) {
        logger.warn("Not enough VE resources allocated for the number of executors specified.")
      }

      veResources.addresses(veMultiple % veResources.addresses.size).toInt
    }

    logger.info(s"Attemping to use VE node = ${selectedNodeId}")

    val tupleO = selectedNodeId.until(MaxVeNodes).foldLeft(Option.empty[(Int, veo_proc_handle, Seq[veo_thr_ctxt])]) {
      case (Some(tuple), venode)  => Some(tuple)
      case (None, venode)         => createVeoTuple(venode, veCores)
    }

    tupleO match {
      case Some((venode, handle, tcontexts)) =>
        val identifier = s"VE Process @ ${handle.address}, Executor ${Try { context.executorID }.getOrElse("UNKNOWN")}"
        WrappingVeo(venode, identifier, handle, tcontexts, context.metricRegistry)

      case None =>
        throw new IllegalArgumentException(s"VE process could not be allocated; all nodes are either offline or occupied by another VE process")
    }
  }
}
