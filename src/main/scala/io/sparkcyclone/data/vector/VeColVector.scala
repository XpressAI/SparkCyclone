package io.sparkcyclone.data.vector

import io.sparkcyclone.cache.VeColColumnarVector
import io.sparkcyclone.data.VeColVectorSource
import io.sparkcyclone.spark.codegen.core._
import io.sparkcyclone.metrics.VeProcessMetrics
import io.sparkcyclone.vectorengine.{VeAsyncResult, VeProcess}
import org.apache.spark.sql.vectorized.ColumnVector
import org.bytedeco.javacpp.BytePointer
import org.bytedeco.veoffload.global.veo
import org.slf4j.LoggerFactory

final case class VeColVector private[vector] (
  source: VeColVectorSource,
  name: String,
  veType: VeType,
  numItems: Int,
  buffers: Seq[Long],
  dataSize: Option[Int],
  container: Long
) extends ColVectorLike {
  private val logger = LoggerFactory.getLogger(getClass)
  private var open = true

  def isOpen: Boolean = {
    open
  }

  def allocations: Seq[Long] = {
    Seq(container) ++ buffers
  }

  def toBytes(implicit process: VeProcess,
              metrics: VeProcessMetrics): Array[Byte] = {
    val bytes = metrics.measureRunningTime {
      toBytePointerColVector.toBytes
    }(metrics.registerSerializationTime)

    assert(
      bytes.length == bufferSizes.sum,
      "Resulting Array[Byte] should be same size as sum of all buffer sizes"
    )

    bytes
  }

  def toBytePointersAsync()(implicit process: VeProcess): Seq[VeAsyncResult[BytePointer]] = {
    buffers.zip(bufferSizes).map { case (location, size) =>
      val buffer = new BytePointer(size)
      val handle = process.getAsync(buffer, location)
      VeAsyncResult(handle) { () => buffer }
    }
  }

  def toSparkColumnVector: ColumnVector = {
    new VeColColumnarVector(Left(this), veType.toSparkType)
  }

  def toBytePointerColVector(implicit process: VeProcess): BytePointerColVector = {
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

  def free()(implicit process: VeProcess): Unit = {
    open.synchronized {
      if (open) {
        require(source == process.source, s"Intended to `free` in ${source}, but got ${process.source} context.")
        allocations.foreach(process.free(_))
        open = false

      } else {
        logger.warn(s"[VE MEMORY ${container}] double free called!")
      }
    }
  }

  /**
   * Mark this VeColVector as closed and return all of its allocations to be
   * freed externally.
   *
   * If it is closed already, log a warning and return an empty seq.
   *
   * @return Seq of allocations
   */
  def closeAndReturnAllocations: Seq[Long] = {
    open.synchronized {
      if (open) {
        open = false
        allocations
      } else {
        logger.warn(s"[VE MEMORY ${container}] double free called!")
        Nil
      }
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
