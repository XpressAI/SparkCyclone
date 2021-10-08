package com.nec.arrow

import java.nio.ByteBuffer
import com.nec.aurora.Aurora
import org.bytedeco.javacpp.LongPointer
import com.nec.arrow.ArrowNativeInterface._
import com.typesafe.scalalogging.LazyLogging

import java.io.FileNotFoundException
import java.nio.file.Files
import java.nio.file.Paths
import com.nec.util.LruVeoMemCache

final class VeArrowNativeInterface(proc: Aurora.veo_proc_handle, lib: Long)
  extends ArrowNativeInterface {
  override def callFunctionWrapped(name: String, arguments: List[NativeArgument]): Unit = {
    VeArrowNativeInterface.executeVe(
      proc = proc,
      lib = lib,
      functionName = name,
      arguments = arguments
    )
  }
}

object VeArrowNativeInterface extends LazyLogging {
  private var libs: Map[String, Long] = Map()
  private var functionAddrs: Map[(Long, String), Long] = Map()

  def requireOk(result: Int): Unit = {
    require(result >= 0, s"Result should be >=0, got $result")
  }

  def requirePositive(result: Long): Unit = {
    require(result > 0, s"Result should be > 0, got $result")
  }

  final class VeArrowNativeInterfaceLazyLib(proc: Aurora.veo_proc_handle, libPath: String)
    extends ArrowNativeInterface {
    override def callFunctionWrapped(name: String, arguments: List[NativeArgument]): Unit = {
      val lib = if (!libs.contains(libPath)) {
        // XXX: Can probably cache more than just 1 library but can't know how much space we have with
        // the current AVEO API.  Caching the last library is sufficient for our purposes now.
        if (libs.nonEmpty) {
          val (libPath, lib) = libs.head
          logger.debug(s"Unloading: $libPath")
          val startUnload = System.currentTimeMillis()
          Aurora.veo_unload_library(proc, lib)
          val unloadTime = System.currentTimeMillis() - startUnload
          logger.debug(s"Unloaded: $libPath in $unloadTime")
          libs -= libPath
          functionAddrs = Map()
        }
        logger.debug(s"Will load: '$libPath' to call '$name'")
        if (!Files.exists(Paths.get(libPath))) {
          throw new FileNotFoundException(s"Required fille $libPath does not exist")
        }
        val startLoad = System.currentTimeMillis()
        val lib = Aurora.veo_load_library(proc, libPath)
        val loadTime = System.currentTimeMillis() - startLoad
        logger.debug(s"Loaded: '$libPath in $loadTime")
        require(lib != 0, s"Expected lib != 0, got $lib")

        libs += (libPath -> lib)
        lib
      } else {
        logger.debug(s"Using cached: '$libPath' to call '$name'")
        libs(libPath)
      }

      new VeArrowNativeInterface(proc, lib).callFunctionWrapped(name, arguments)
    }
  }

  class Cleanup(var items: List[Long] = Nil) {
    def add(long: Long, size: Long): Unit = items = {
      logger.debug(s"Adding to clean-up: $long, $size bytes")
      long :: items
    }
  }

  def copyBufferToVe(
    proc: Aurora.veo_proc_handle,
    byteBuffer: ByteBuffer,
    len: Option[Long] = None
  )(implicit cleanup: Cleanup): Long = {
    val veInputPointer = new LongPointer(8)

    /** No idea why Arrow in some cases returns a ByteBuffer with 0-capacity, so we have to pass a length explicitly! */
    val size = len.getOrElse(byteBuffer.capacity().toLong)
    requireOk(Aurora.veo_alloc_mem(proc, veInputPointer, size))
    requireOk(
      Aurora.veo_write_mem(
        proc,
        /** after allocating, this pointer now contains a value of the VE storage address * */
        veInputPointer.get(),
        new org.bytedeco.javacpp.Pointer(byteBuffer),
        size
      )
    )
    veInputPointer.get()
    val ptr = veInputPointer.get()
    cleanup.add(ptr, size)
    ptr
  }

  private def executeVe(
    proc: Aurora.veo_proc_handle,
    lib: Long,
    functionName: String,
    arguments: List[NativeArgument]
  ): Unit = {
    assert(lib > 0, s"Expected lib to be >0, was $lib")
    val our_args = Aurora.veo_args_alloc()
    implicit val cleanup: Cleanup = new Cleanup()
    try {

      val transferBack = scala.collection.mutable.Buffer.empty[() => Unit]
      arguments.zipWithIndex.foreach {
        case (NativeArgument.ScalarInputNativeArgument(ScalarInput.ForInt(num)), idx) =>
          requireOk(Aurora.veo_args_set_i32(our_args, idx, num))
        case (NativeArgument.VectorInputNativeArgument(wrapper), index) =>
          VeArrowTransfers.transferInput(proc, our_args, wrapper, index)
        case (NativeArgument.VectorOutputNativeArgument(wrapper), index) =>
          val transferF = VeArrowTransfers.transferOutput(proc, our_args, wrapper, index)

          transferBack.append(() => {
            try transferF()
            catch {
              case e: Throwable =>
                throw new RuntimeException(
                  s"Failed to transfer back for index ${index}, type ${wrapper.valueVector
                    .getClass()}: $e",
                  e
                )
            }
          })
      }

      val startTime = System.currentTimeMillis()
      val uuid = java.util.UUID.randomUUID()

      logger.debug(s"[$uuid] Starting VE call to '$functionName'...")
      val fnAddr = if (functionAddrs.contains((lib, functionName))) {
        functionAddrs((lib, functionName))
      } else {
        val addr = Aurora.veo_get_sym(proc, lib, functionName)
        functionAddrs += ((lib, functionName) -> addr)
        addr
      }

      val fnCallResult = new LongPointer(8)
      val callRes = Aurora.veo_call_sync(proc, fnAddr, our_args, fnCallResult)
      val time = System.currentTimeMillis() - startTime
      logger.debug(
        s"[$uuid] Got result from VE call to '$functionName': '$callRes'. Took ${time}ms"
      )

      require(
        callRes == 0,
        s"Expected 0, got $callRes; means VE call failed for function $functionName; args: $arguments"
      )

      require(fnCallResult.get() == 0L, s"Expected 0, got ${fnCallResult.get()} back instead.")

      try transferBack.foreach(_.apply())
      catch {
        case e: Throwable =>
          val inputs = arguments.collect { case NativeArgument.VectorInputNativeArgument(wrapper) =>
            wrapper
          }

          val types = arguments.zipWithIndex.map {
            case (
                  NativeArgument.VectorInputNativeArgument(
                    wrapper: NativeArgument.VectorInputNativeArgument.InputVectorWrapper.InputArrowVectorWrapper
                  ),
                  idx
                ) =>
              s"${idx} => ${wrapper.valueVector.getClass()}"
            case (NativeArgument.VectorOutputNativeArgument(wrapper), idx) =>
              s"${idx} => ${wrapper.valueVector.getClass()}"
            case (other, idx) =>
              s"${idx} => ${other.getClass()}"
          }

          throw new RuntimeException(
            s"Failed to transfer back due to $e; inputs were = ${inputs}; function was ${functionName}; types were ${types}",
            e
          )
      }
    } finally {
      val cleanupResult = cleanup.items.map(ptr => ptr -> Aurora.veo_free_mem(proc, ptr))
      if (cleanupResult.exists(_._2 < 0)) {
        logger.error(s"Clean-up failed for some cases: $cleanupResult")
      }
      Aurora.veo_args_free(our_args)
    }
  }
}
