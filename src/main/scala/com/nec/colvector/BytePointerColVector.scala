package com.nec.colvector

import com.nec.spark.agile.core.{VeScalarType, VeString, VeType}
import com.nec.util.CallContext
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.{VeAsyncResult, VeProcess, VeProcessMetrics}
import com.nec.vectorengine.{VeProcess => NewVeProcess, VeAsyncResult => NewVeAsyncResult}
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

        // The layout of `nullable_T_vector` is the same for all T = primitive
        // Assign the data, validity, and count values
        new BytePointer(stype.containerSize.toLong)
          .putLong(0, veMemoryPositions(1))
          .putLong(8, veMemoryPositions(2))
          .putInt(16, numItems.abs)

      case VeString =>
        require(buffers.size == 4, s"Exactly 4 VE buffer pointers are required to construct container for ${VeString}")
        require(dataSize.nonEmpty, s"dataSize is required to construct container for ${VeString}")
        val Some(actualDataSize) = dataSize

        // Assign the data, validity, starts, lens, and count values
        new BytePointer(VeString.containerSize.toLong)
          .putLong(0,  veMemoryPositions(1))
          .putLong(8,  veMemoryPositions(2))
          .putLong(16, veMemoryPositions(3))
          .putLong(24, veMemoryPositions(4))
          .putInt(32,  actualDataSize.abs)
          .putInt(36,  numItems.abs)
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

  def asyncToVeColVector2(implicit process: NewVeProcess): () => NewVeAsyncResult[VeColVector] = {
    // Allocate the buffers on the VE
    val veLocations = (Seq(veType.containerSize.toLong) ++ buffers.map(_.limit()))
      .map(process.allocate)
      .map(_.address)

    // Create the nullable_T_vector struct on VH
    val struct = veType match {
      case stype: VeScalarType =>
        require(buffers.size == 2, s"Exactly 2 VE buffer pointers are required to construct container for ${stype}")

        // The layout of `nullable_T_vector` is the same for all T = primitive
        // Assign the data, validity, and count values
        new BytePointer(stype.containerSize.toLong)
          .putLong(0, veLocations(1))
          .putLong(8, veLocations(2))
          .putInt(16, numItems.abs)

      case VeString =>
        require(buffers.size == 4, s"Exactly 4 VE buffer pointers are required to construct container for ${VeString}")
        require(dataSize.nonEmpty, s"dataSize is required to construct container for ${VeString}")
        val Some(actualDataSize) = dataSize

        // Assign the data, validity, starts, lens, and count values
        new BytePointer(VeString.containerSize.toLong)
          .putLong(0,  veLocations(1))
          .putLong(8,  veLocations(2))
          .putLong(16, veLocations(3))
          .putLong(24, veLocations(4))
          .putInt(32,  actualDataSize.abs)
          .putInt(36,  numItems.abs)
    }

    val vector = VeColVector(
      process.source,
      name,
      veType,
      numItems,
      veLocations.tail,
      dataSize,
      veLocations.head
    )

    () => {
      val handles = (Seq(struct) ++ buffers).zipWithIndex.map { case (ptr, idx) =>
        process.putAsync(ptr, veLocations(idx))
      }
      NewVeAsyncResult(handles: _*) { () =>
        struct.close
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

  def toVeColVector2(implicit process: NewVeProcess): VeColVector = {
    asyncToVeColVector2.apply.get
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
