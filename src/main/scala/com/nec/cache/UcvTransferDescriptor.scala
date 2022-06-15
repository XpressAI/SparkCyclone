package com.nec.cache

import com.nec.colvector._
import com.nec.spark.agile.core.{VeScalarType, VeString}
import com.typesafe.scalalogging.LazyLogging
import org.bytedeco.javacpp.{BytePointer, LongPointer}

case class UcvTransferDescriptor(batch: Seq[UnitColVector],
                                 val buffer: BytePointer)
                                 extends TransferDescriptor with LazyLogging {
  require(buffer.address > 0L, s"Buffer has an invalid address ${buffer.address}; either it is un-initialized or already closed")

  lazy val isEmpty: Boolean = {
    batch.isEmpty
  }

  lazy val nonEmpty: Boolean = {
    !isEmpty
  }

  private[cache] lazy val nbatches: Long = {
    batch.size.toLong
  }

  private[cache] lazy val ncolumns: Long = {
    batch.headOption.map(_.numItems.toLong).getOrElse(0L)
  }

  private[cache] lazy val resultOffsets: Seq[Long] = {
    batch.map(_.veType)
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

  lazy val resultBuffer: LongPointer = {
    require(nbatches > 0, "Need more than 0 batches for creating a result buffer!")
    require(ncolumns > 0, "Need more than 0 columns for creating a result buffer!")

    // Total size of the buffer is computed from scan-left of the result sizes
    logger.debug(s"Allocating transfer output pointer of ${resultOffsets.last} bytes")
    new LongPointer(resultOffsets.last)
  }

  def resultToColBatch(implicit source: VeColVectorSource): VeColBatch = {
    val vcolumns = batch.zipWithIndex.map { case (column, i) =>
      logger.debug(s"Reading output pointers for column ${i}")

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
          Some(column.dataSize.getOrElse(0))
      }

      VeColVector(
        source,
        column.name,
        column.veType,
        column.numItems,
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
}
