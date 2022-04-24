package com.nec.colvector

import com.nec.cache.VeColColumnarVector
import com.nec.spark.agile.core.VeType
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.{VeAsyncResult, VeProcess, VeProcessMetrics}
import org.apache.spark.sql.vectorized.ColumnVector
import org.bytedeco.javacpp.BytePointer
import org.bytedeco.veoffload.global.veo
import org.slf4j.LoggerFactory

final case class VeColVector private[colvector] (
  source: VeColVectorSource,
  name: String,
  veType: VeType,
  numItems: Int,
  buffers: Seq[Long],
  dataSize: Option[Int],
  container: Long
) extends ColVectorUtilsTrait {
  private val logger = LoggerFactory.getLogger(getClass)
  private var memoryFreed = false

  def allocations: Seq[Long] = {
    Seq(container) ++ buffers
  }

  def toBytes(implicit process: VeProcess,
                metrics: VeProcessMetrics): Array[Byte] = {
    val bytes = metrics.measureRunningTime {
      // toBytePointerColVector.toByteArrayColVector.serialize
      toBytePointerColVector.toBytes
    }(metrics.registerSerializationTime)

    assert(
      bytes.length == bufferSizes.sum,
      "Resulting Array[Byte] should be same size as sum of all buffer sizes"
    )

    bytes
  }

  def toBytePointersAsync()(implicit process: VeProcess): Seq[VeAsyncResult[BytePointer]] = {
    import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext
    buffers.zip(bufferSizes).map { case (start, size) =>
      val bp = new BytePointer(size)
      val handle = process.getAsync(bp, start, size)
      VeAsyncResult(handle){() => bp}
    }
  }

  def toSparkColumnVector: ColumnVector = {
    new VeColColumnarVector(Left(this), veType.toSparkType)
  }

  def toBytePointerColVector(implicit process: VeProcess): BytePointerColVector = {
    val nbuffers = buffers.zip(bufferSizes).map { case (location, size) =>
      val ptr = new BytePointer(size)
      val handle = process.getAsync(ptr, location, size)
      (ptr, handle)
    }.map{ case (ptr, handle) =>
      require(process.waitResult(handle)(null)._1 == veo.VEO_COMMAND_OK)
      ptr
    }

    BytePointerColVector(
      source,
      name,
      veType,
      numItems,
      nbuffers
    )
  }

  def toUnitColVector: UnitColVector = {
    UnitColVector(
      source,
      name,
      veType,
      numItems,
      dataSize
    )
  }

  def free()(implicit dsource: VeColVectorSource,
             process: VeProcess,
             context: OriginalCallingContext): Unit = {
    if (memoryFreed) {
      logger.warn(s"[VE MEMORY ${container}] double free called!")

    } else {
      require(dsource == source, s"Intended to `free` in ${source}, but got ${dsource} context.")
      allocations.foreach(process.free)
      memoryFreed = true
    }
  }
}
