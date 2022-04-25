package com.nec.colvector

import com.nec.spark.agile.core.{VeScalarType, VeString, VeType}
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.{VeProcess, VeProcessMetrics}
import org.bytedeco.javacpp.BytePointer

final case class BytePointerColVector private[colvector] (
  source: VeColVectorSource,
  name: String,
  veType: VeType,
  numItems: Int,
  buffers: Seq[BytePointer],
) {
  require(
    numItems >= 0,
    s"[${getClass.getName}] numItems should be >= 0"
  )

  require(
    buffers.size == (if (veType == VeString) 4 else 2),
    s"[${getClass.getName}] Number of BytePointer's does not match the requirement for ${veType}"
  )

  def dataSize: Option[Int] = {
    veType match {
      case _: VeScalarType =>
        None

      case VeString =>
        Some(buffers(0).limit().toInt / 4)
    }
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

  def toVeColVector(implicit source: VeColVectorSource,
                    process: VeProcess,
                    context: OriginalCallingContext,
                    metrics: VeProcessMetrics): VeColVector = {
    val nbuffers = metrics.measureRunningTime {
      buffers.map(process.putPointer)
    }(metrics.registerTransferTime)

    val container = VeColVector.buildContainer(
      veType,
      numItems,
      nbuffers,
      dataSize
    )

    VeColVector(
      source,
      name,
      veType,
      numItems,
      nbuffers,
      dataSize,
      container
    )
  }

  def toByteArrayColVector: ByteArrayColVector = {
    val nbuffers = buffers.map { ptr =>
      try {
        ptr.asBuffer.array

      } catch {
        case _: UnsupportedOperationException =>
          val output = Array.fill[Byte](ptr.limit().toInt)(-1)
          ptr.get(output)
          output
      }
    }

    ByteArrayColVector(
      source,
      name,
      veType,
      numItems,
      nbuffers
    )
  }

  def toBytes: Array[Byte] = {
    val bufferSizes = buffers.map(_.limit().toInt)
    val bytes = Array.ofDim[Byte](bufferSizes.foldLeft(0)(_ + _))

    (buffers, bufferSizes.scanLeft(0)(_ + _), bufferSizes).zipped.foreach {
      case (buffer, start, size) => buffer.get(bytes, start, size)
    }

    bytes
  }
}
