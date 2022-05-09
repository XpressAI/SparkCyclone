package com.nec.colvector

import com.nec.spark.agile.core.{VeScalarType, VeString, VeType}
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.{VeAsyncResult, VeProcess, VeProcessMetrics}
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

  def asyncToVeColVector(implicit source: VeColVectorSource,
                         process: VeProcess,
                         context: OriginalCallingContext,
                         metrics: VeProcessMetrics): () => VeAsyncResult[VeColVector] = {
    val veMemoryPositions = (Seq(veType.containerSize.toLong) ++ buffers.map(_.limit())).map(process.allocate)
    val structPtr = veType match {
      case stype: VeScalarType =>
        require(buffers.size == 2, s"Exactly 2 VE buffer pointers are required to construct container for ${stype}")
        // Declare the struct in host memory
        // The layout of `nullable_T_vector` is the same for all T = primitive
        val ptr = new BytePointer(stype.containerSize.toLong)

        // Assign the data, validity, and count values
        ptr.putLong(0, veMemoryPositions(1))
        ptr.putLong(8, veMemoryPositions(2))
        ptr.putInt(16, numItems.abs)

        ptr
      case VeString =>
        require(buffers.size == 4, s"Exactly 4 VE buffer pointers are required to construct container for ${VeString}")
        require(dataSize.nonEmpty, s"dataSize is required to construct container for ${VeString}")
        val Some(actualDataSize) = dataSize
        // Declare the struct in host memory
        val ptr = new BytePointer(VeString.containerSize.toLong)

        // Assign the data, validity, and count values
        ptr.putLong(0,  veMemoryPositions(1))
        ptr.putLong(8,  veMemoryPositions(2))
        ptr.putLong(16, veMemoryPositions(3))
        ptr.putLong(24, veMemoryPositions(4))
        ptr.putInt(32,  actualDataSize.abs)
        ptr.putInt(36,  numItems.abs)

        ptr
    }

    // TODO: Register this vector's allocation.
    val vector = VeColVector(
      source,
      name,
      veType,
      numItems,
      veMemoryPositions.tail,
      dataSize,
      veMemoryPositions.head
    )

    () => {
      val handles = (Seq(structPtr) ++ buffers).zipWithIndex.map{case (ptr, idx) =>
        process.putAsync(ptr, veMemoryPositions(idx))
      }
      VeAsyncResult(handles){ () =>
        structPtr.close()
        vector
      }
    }
  }

  def toVeColVector(implicit source: VeColVectorSource,
                    process: VeProcess,
                    context: OriginalCallingContext,
                    metrics: VeProcessMetrics): VeColVector = {
    asyncToVeColVector.apply().get()
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
