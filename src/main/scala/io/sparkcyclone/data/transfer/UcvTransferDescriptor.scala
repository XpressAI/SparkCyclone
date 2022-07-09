package io.sparkcyclone.data.transfer

import io.sparkcyclone.data.vector._
import io.sparkcyclone.data.VeColVectorSource
import io.sparkcyclone.native.code.{VeScalarType, VeString}
import com.typesafe.scalalogging.LazyLogging
import org.bytedeco.javacpp.{BytePointer, LongPointer}

case class UcvTransferDescriptor(columns: Seq[UnitColVector],
                                 val buffer: BytePointer)
                                 extends TransferDescriptor with LazyLogging {
  require(buffer.address > 0L, s"Buffer has an invalid address ${buffer.address}; either it is un-initialized or already closed")

  lazy val nonEmpty: Boolean = {
    columns.nonEmpty
  }

  private[transfer] lazy val resultOffsets: Seq[Long] = {
    TransferDescriptor.resultOffsets(columns)
  }

  lazy val resultBuffer: LongPointer = {
    // Total size of the buffer is computed from scan-left of the result sizes
    logger.debug(s"Allocating transfer output pointer of ${resultOffsets.last} bytes")
    new LongPointer(resultOffsets.last)
  }

  def resultToColBatch(implicit source: VeColVectorSource): VeColBatch = {
    val vcolumns = columns.zipWithIndex.map { case (column, i) =>
      logger.debug(s"Reading output pointers for column ${i}")

      // The first offset is the pointer to the container struct
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
