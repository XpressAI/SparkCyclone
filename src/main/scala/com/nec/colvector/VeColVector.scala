package com.nec.colvector

import com.nec.cache.VeColColumnarVector
import com.nec.spark.agile.core._
import com.nec.util.CallContext
import com.nec.ve.{VeAsyncResult, VeProcess, VeProcessMetrics}
import com.nec.vectorengine.{VeProcess => NewVeProcess}
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
   import com.nec.util.CallContextOps._
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

  def toBytePointerColVector2(implicit process: NewVeProcess): BytePointerColVector = {
    val nbuffers = buffers.zip(bufferSizes)
      .map { case (location, size) =>
        val buffer = new BytePointer(size)
        (buffer, process.getAsync(buffer, location))
      }
      .map{ case (buffer, handle) =>
        require(process.awaitResult(handle).get == veo.VEO_COMMAND_OK)
        buffer
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
             context: CallContext): Unit = {
    if (memoryFreed) {
      logger.warn(s"[VE MEMORY ${container}] double free called!")

    } else {
      require(dsource == source, s"Intended to `free` in ${source}, but got ${dsource} context.")
      allocations.foreach(process.free)
      memoryFreed = true
    }
  }

  def free2(implicit process: NewVeProcess): Unit = {
    if (memoryFreed) {
      logger.warn(s"[VE MEMORY ${container}] double free called!")

    } else {
      require(source == process.source, s"Intended to `free` in ${source}, but got ${process.source} context.")
      allocations.foreach(process.free(_))
      memoryFreed = true
    }
  }
}


object VeColVector {
  def fromBuffer(buffer: BytePointer,
                 velocation: Long,
                 descriptor: CVector)(implicit source: VeColVectorSource): VeColVector = {
    descriptor match {
      case CVarChar(name) =>
        VeColVector(
          source,
          name,
          VeString,
          buffer.getInt(36),
          Seq(
            buffer.getLong(0),
            buffer.getLong(8),
            buffer.getLong(16),
            buffer.getLong(24)
          ),
          Some(buffer.getInt(32)),
          velocation
        )

    case CScalarVector(name, stype) =>
      VeColVector(
        source,
        name,
        stype,
        buffer.getInt(16),
        Seq(
          buffer.getLong(0),
          buffer.getLong(8)
        ),
        None,
        velocation
      )
    }
  }
}
