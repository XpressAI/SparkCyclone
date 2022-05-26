package com.nec.cache

import com.nec.colvector.{BytePointerColVector, VeColBatch, VeColVector}
import com.nec.spark.agile.core.{VeScalarType, VeString}
import com.nec.util.PointerOps._
import com.typesafe.scalalogging.LazyLogging
import org.bytedeco.javacpp.{BytePointer, LongPointer, Pointer}

case class TransferDescriptor2(batches: Seq[Seq[BytePointerColVector]]) extends LazyLogging {
  lazy val isEmpty: Boolean = {
    batches.flatten.isEmpty
  }

  lazy val nonEmpty: Boolean = {
    !isEmpty
  }

  lazy val nbatches: Long = {
    batches.size.toLong
  }

  lazy val ncolumns: Long = {
    batches.headOption.map(_.size.toLong).getOrElse(0L)
  }

  private[cache] lazy val batchwiseColumns: Seq[Seq[BytePointerColVector]] = {
    batches.transpose
  }

  private[cache] lazy val columns: Seq[BytePointerColVector] = {
    // Transpose the columns such that the first column of each batch comes first, followed by the second column of each batch, etc.
    batchwiseColumns.flatten
  }

  private[cache] lazy val headerOffsets: Seq[Long] = {
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

  private[cache] lazy val dataOffsets: Seq[Long] = {
    columns.flatMap(_.buffers)
      // Get the size of each buffer in bytes
      .map { buf => vectorAlignedSize(buf.limit) }
      // Start the accumulation from header total size
      // Offsets are in bytes
      .scanLeft(headerOffsets.last * 8)(_ + _)
  }

  private[cache] lazy val resultOffsets: Seq[Long] = {
    batches.head.map(_.veType)
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

  private[cache] def vectorAlignedSize(size: Long): Long = {
    val dangling = size % 8
    if (dangling > 0) {
      // If the size is not aligned on 8 bytes, add some padding
      size + (8 - dangling)
    } else {
      size
    }
  }

  lazy val buffer: BytePointer = {
    require(nbatches > 0, "Need more than 0 batches for transfer!")
    require(ncolumns > 0, "Need more than 0 columns for transfer!")
    require(batches.forall(_.size == ncolumns), "All batches must have the same column count!")
    logger.debug(s"Preparing transfer buffer for ${nbatches} batches of ${ncolumns} columns")

    // Total size of the buffer is computed from scan-left of the header and data sizes
    val tsize = (headerOffsets.last * 8) + dataOffsets.last

    logger.debug(s"Allocating transfer buffer of ${tsize} bytes")
    val outbuffer = new BytePointer(tsize)
    val header = new LongPointer(outbuffer)

    // Zero out the memory for consistency
    Pointer.memset(outbuffer, 0, outbuffer.limit)

    // Write the descriptor header
    // Total header size is in bytes
    header.put(0, headerOffsets.last * 8)
    header.put(1, nbatches)
    header.put(2, ncolumns)

    // Write the column headers
    columns.zipWithIndex.foreach { case (column, i) =>
      val buffers = column.buffers.toList
      val start = headerOffsets(i)

      header.put(start, column.veType.cEnumValue)
      header.put(start + 1, column.numItems)
      header.put(start + 2, buffers(0).limit())
      header.put(start + 3, buffers(1).limit())

      if (column.veType.isString) {
        header.put(start + 4, buffers(2).limit())
        header.put(start + 5, buffers(3).limit())
      }
    }

    // Write the data from the individual column buffers
    columns.flatMap(_.buffers).zipWithIndex.foreach { case (buf, i) =>
      val start = dataOffsets(i)
      Pointer.memcpy(outbuffer.position(start), buf, buf.limit)
    }

    outbuffer.position(0)
  }

  lazy val resultBuffer: LongPointer = {
    require(nbatches > 0, "Need more than 0 batches for creating a result buffer!")
    require(ncolumns > 0, "Need more than 0 columns for creating a result buffer!")

    // Total size of the buffer is computed from scan-left of the result sizes
    logger.debug(s"Allocating transfer output pointer of ${resultOffsets.last} bytes")
    new LongPointer(resultOffsets.last)
  }

  def resultToColBatch: VeColBatch = {
    val vcolumns = batches.head.zipWithIndex.map { case (column, i) =>
      logger.debug(s"Reading output pointers for column ${i}")

      val batch = batchwiseColumns(1)
      val cbuf = resultBuffer.position(resultOffsets(i))

      // Fetch the pointers to the nullable_t_vector
      val buffers = (if (column.veType.isString) 1.to(4) else 1.to(2))
        .toSeq
        .map(cbuf.get(_))

      // Compute the dataSize
      val dataSizeO = column.veType match {
        case _: VeScalarType =>
          None
        case VeString =>
          Some(batch.map(_.dataSize).flatten.foldLeft(0)(_ + _))
      }

      VeColVector(
        column.source,
        column.name,
        column.veType,
        // Compute the total size of the column
        batch.map(_.numItems).sum,
        buffers,
        dataSizeO,
        cbuf.get(0)
      )
    }

    VeColBatch(vcolumns)
  }

  def close: Unit = {
    buffer.close
    resultBuffer.close
  }

  def toSeq: Seq[Byte] = {
    val buf = buffer
    val array = Array.ofDim[Byte](buffer.limit().toInt)
    buf.get(array)
    array.toSeq
  }

  def print: Unit = {
    println(s"Transfer Buffer = \n${buffer.hexdump}\n")
  }
}
