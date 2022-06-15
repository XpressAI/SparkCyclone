package com.nec.cache

import com.nec.colvector.{UnitColVector, VeColBatch, VeColVectorSource}
import com.nec.spark.agile.core.{VeScalarType, VeString}
import com.nec.util.PointerOps.ExtendedPointer
import org.bytedeco.javacpp.{BytePointer, LongPointer}

trait TransferDescriptor {
  def buffer: BytePointer
  def resultBuffer: LongPointer
  def resultToColBatch(implicit source: VeColVectorSource): VeColBatch
  def close: Unit
  def nonEmpty: Boolean

  def print: Unit = {
    println(s"Transfer Buffer = \n${buffer.hexdump}\n")
  }

  def toSeq: Seq[Byte] = {
    val buf = buffer
    val array = Array.ofDim[Byte](buffer.limit().toInt)
    buf.get(array)
    array.toSeq
  }
}

object TransferDescriptor {
  def vectorAlignedSize(size: Long): Long = {
    val dangling = size % 8
    if (dangling > 0) {
      // If the size is not aligned on 8 bytes, add some padding
      size + (8 - dangling)
    } else {
      size
    }
  }

  def bufferSize(columns: Seq[UnitColVector]): Long = {
    require(columns.nonEmpty, "Need more than 1 columns to compute buffer size!")

    val headerOffsets: Seq[Long] = {
      columns.map(_.veType)
        .map {
          case _: VeScalarType =>
            // The header info for a scalar column contains 4 uint64_t values:
            // [column_type][element_count][data_size][validity_buffer_size]
            4L

          case VeString =>
            // The header info for a scalar column contains 6 uint64_t values:
            // [column_type][element_count][data_size][offsets_size][lengths_size][validity_buffer_size]
            6L
        }
        .scanLeft(3L)(_ + _)
      }

    val dataOffsets: Seq[Long] = {
      columns.flatMap(_.bufferSizes)
        // Get the size of each buffer in bytes
        .map { bufsize => vectorAlignedSize(bufsize.toLong) }
        // Start the accumulation from header total size
        // Offsets are in bytes
        .scanLeft(headerOffsets.last * 8)(_ + _)
    }

    // Total size of the buffer is computed from scan-left of the header and data sizes
    (headerOffsets.last * 8) + dataOffsets.last
  }
}
