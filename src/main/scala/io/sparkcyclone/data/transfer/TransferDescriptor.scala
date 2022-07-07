package io.sparkcyclone.data.transfer

import io.sparkcyclone.colvector.{ColVectorLike, UnitColVector, VeColBatch, VeColVectorSource}
import io.sparkcyclone.spark.agile.core.{VeScalarType, VeString}
import io.sparkcyclone.util.PointerOps.ExtendedPointer
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

  def headerOffsets(columns: Seq[ColVectorLike]): Seq[Long] = {
    require(columns.nonEmpty, "Need at least 1 column to compute buffer header offsets!")

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
      // The transfer descriptor header contains 3 uint64_t values:
      // [header_size, batch_count, column_count]
      // Offsets are in uint64_t
      .scanLeft(3L)(_ + _)
  }

  def dataOffsets(columns: Seq[ColVectorLike]): Seq[Long] = {
    require(columns.nonEmpty, "Need at least 1 column to compute buffer data offsets!")

    columns.flatMap(_.bufferSizes)
      .map(_.toLong)
      // Get the size of each buffer in bytes
      .map(vectorAlignedSize)
      // Start the accumulation from header total size
      // Offsets are in bytes
      .scanLeft(headerOffsets(columns).last * 8)(_ + _)
  }

  def bufferSize(columns: Seq[ColVectorLike]): Long = {
    // Total size of the buffer is computed from scan-left of the header and data sizes
    dataOffsets(columns).last
  }

  def resultOffsets(columns: Seq[ColVectorLike]): Seq[Long] = {
    require(columns.nonEmpty, "Need at least 1 column to compute result buffer offsets!")

    columns.map(_.veType)
      .map {
        case _: VeScalarType =>
          // scalar vectors prodduce 3 pointers (struct, data buffer, validity buffer)
          3L

        case VeString =>
          // nullable_varchar_vector produce 5 pointers (struct, data buffer, offsets, lengths, validity buffer)
          5L
      }
      // Accumulate the offsets (offsets are in uint64_t)
      .scanLeft(0L)(_ + _)
  }
}
