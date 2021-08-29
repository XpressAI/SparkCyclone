package com.nec.spark

import com.nec.arrow.VeArrowNativeInterface
import com.nec.arrow.VeArrowNativeInterfaceNumeric
import com.nec.aurora.Aurora
import com.nec.aurora.Aurora.veo_proc_handle

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter
import com.nec.spark.Aurora4SparkExecutorPlugin._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.internal.Logging

import java.nio.file.Files
import java.nio.file.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Aurora4SparkExecutorPlugin {

  /** For assumption testing purposes only for now */
  var params: Map[String, String] = Map.empty[String, String]

  /** For assumption testing purposes only for now */
  private[spark] var launched: Boolean = false
  var _veo_proc: veo_proc_handle = _
  var lib: Long = -1
  var veArrowNativeInterface: VeArrowNativeInterface = _
  var veArrowNativeInterfaceNumeric: VeArrowNativeInterfaceNumeric = _

  /**
   * https://www.hpc.nec/documents/veos/en/veoffload/md_Restriction.html
   *
   * VEO does not support:
   * to use quadruple precision real number a variable length character string as a return value and an argument of Fortran subroutines and functions,
   * to use multiple VEs by a VH process,
   * to re-create a VE process after the destruction of the VE process, and
   * to call API of VE DMA or VH-VE SHM on the VE side if VEO API is called from child thread on the VH side.
   *
   * *
   */
  var closeAutomatically: Boolean = false
  def closeProcAndCtx(): Unit = {
    if (_veo_proc != null) {
      Aurora.veo_proc_destroy(_veo_proc)
    }
  }

  var DefaultVeNodeId = 0

  trait LibraryStorage {
    // Get a local copy of the library for loading
    def getLocalLibraryPath(code: String, libraryData: Vector[Byte]): Path
  }

  final class SimpleLibraryStorage extends LibraryStorage with LazyLogging {

    private var locallyStoredLibs = Map.empty[String, Path]

    /** Get a local copy of the library for loading */
    override def getLocalLibraryPath(code: String, libraryData: Vector[Byte]): Path =
      this.synchronized {
        locallyStoredLibs.get(code) match {
          case Some(result) =>
            logger.debug("Cache hit for executor-fetch for code.")
            result
          case None =>
            logger.debug("Cache miss for executor-fetch for code; asking Driver.")
            val localPath = Files.createTempFile("ve_fn", ".lib")
            Files.write(localPath, libraryData.toArray)
            logger.debug(s"Saved file to '$localPath'")
            locallyStoredLibs += code -> localPath
            localPath
        }
      }
  }

  var libraryStorage: LibraryStorage = _
}

class Aurora4SparkExecutorPlugin extends Logging {

  def init(extraConf: util.Map[String, String]): Unit = {
    println(s"WEEEE \{1/0}")
    val selectedVeNodeId = DefaultVeNodeId

    logInfo(s"Using VE node = ${selectedVeNodeId}")

    Aurora4SparkExecutorPlugin.libraryStorage =
      new Aurora4SparkExecutorPlugin.SimpleLibraryStorage()

    if (_veo_proc == null) {
      _veo_proc = Aurora.veo_proc_create(selectedVeNodeId)
      require(
        _veo_proc != null,
        s"Proc could not be allocated for node ${selectedVeNodeId}, got null"
      )
      require(_veo_proc.address() != 0, s"Address for 0 for proc was ${_veo_proc}")
      logInfo(s"Opened process: ${_veo_proc}")

      /**
       * We currently do two approaches - one is to pre-compile, and another is to compile at the point of the SQL.
       * We're moving to the latter, however this is retained for compatibility for the previous set of sets we had.
       * *
       */
      if (extraConf.containsKey("ve_so_name")) {
        Aurora4SparkExecutorPlugin.lib =
          Aurora.veo_load_library(_veo_proc, extraConf.get("ve_so_name"))
      }
      veArrowNativeInterface = new VeArrowNativeInterface(_veo_proc, lib)
      veArrowNativeInterfaceNumeric = new VeArrowNativeInterfaceNumeric(_veo_proc, lib)
    }
    logInfo("Initializing Aurora4SparkExecutorPlugin.")
    params = params ++ extraConf.asScala
    launched = true
  }

  def shutdown(): Unit = {
    if (closeAutomatically) {
      logInfo(s"Closing process: ${_veo_proc}")
      closeProcAndCtx()
    }
  }
}
