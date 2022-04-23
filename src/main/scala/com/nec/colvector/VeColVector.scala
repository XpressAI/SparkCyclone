package com.nec.colvector

import com.nec.cache.VeColColumnarVector
import com.nec.spark.agile.core.{VeScalarType, VeString, VeType}
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.{VeProcess, VeProcessMetrics}
import org.apache.spark.sql.vectorized.ColumnVector
import org.bytedeco.javacpp.BytePointer
import org.bytedeco.veoffload.global.veo
import org.slf4j.LoggerFactory

import java.io.OutputStream

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

  def toStream(stream: OutputStream)(implicit process: VeProcess): Unit = {
    buffers.zip(bufferSizes).foreach { case (start, size) =>
      process.writeToStream(stream, start, size)
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

object VeColVector {
  def buildContainer(veType: VeType,
                     count: Int,
                     buffers: Seq[Long],
                     dataSizeO: Option[Int])
                    (implicit source: VeColVectorSource,
                     process: VeProcess,
                     context: OriginalCallingContext): Long = {
    veType match {
      case stype: VeScalarType =>
        require(buffers.size == 2, s"Exactly 2 VE buffer pointers are required to construct container for ${stype}")

        // Declare the struct in host memory
        // The layout of `nullable_T_vector` is the same for all T = primitive
        val ptr = new BytePointer(stype.containerSize.toLong)

        // Assign the data, validity, and count values
        ptr.putLong(0, buffers(0))
        ptr.putLong(8, buffers(1))
        ptr.putInt(16, count.abs)

        // Copy the struct to VE and return the VE pointer
        process.putPointer(ptr)

      case VeString =>
        require(buffers.size == 4, s"Exactly 4 VE buffer pointers are required to construct container for ${VeString}")
        require(dataSizeO.nonEmpty, s"dataSize is required to construct container for ${VeString}")
        val Some(dataSize) = dataSizeO

        // Declare the struct in host memory
        val ptr = new BytePointer(VeString.containerSize.toLong)

        // Assign the data, validity, and count values
        ptr.putLong(0,  buffers(0))
        ptr.putLong(8,  buffers(1))
        ptr.putLong(16, buffers(2))
        ptr.putLong(24, buffers(3))
        ptr.putInt(32,  dataSize.abs)
        ptr.putInt(36,  count.abs)

        // Copy the struct to VE and return the VE pointer
        process.putPointer(ptr)
    }
  }
}
