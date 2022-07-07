package io.sparkcyclone.data.transfer

import io.sparkcyclone.colvector.{BytePointerColVector, VeColBatch, VeColVector, VeColVectorSource}
import io.sparkcyclone.spark.agile.core.{VeScalarType, VeString}
import com.typesafe.scalalogging.LazyLogging
import scala.collection.mutable.ListBuffer
import org.bytedeco.javacpp.{BytePointer, LongPointer, Pointer}

case class BpcvTransferDescriptor(batches: Seq[Seq[BytePointerColVector]])
  extends TransferDescriptor with LazyLogging {
  lazy val isEmpty: Boolean = {
    batches.flatten.isEmpty
  }

  lazy val nonEmpty: Boolean = {
    !isEmpty
  }

  private[transfer] lazy val nbatches: Long = {
    batches.size.toLong
  }

  private[transfer] lazy val ncolumns: Long = {
    batches.headOption.map(_.size.toLong).getOrElse(0L)
  }

  private[transfer] lazy val batchwiseColumns: Seq[Seq[BytePointerColVector]] = {
    batches.transpose
  }

  private[transfer] lazy val columns: Seq[BytePointerColVector] = {
    // Transpose the columns such that the first column of each batch comes first,
    // followed by the second column of each batch, etc.
    batchwiseColumns.flatten
  }

  private[transfer] lazy val headerOffsets: Seq[Long] = {
    TransferDescriptor.headerOffsets(columns)
  }

  private[transfer] lazy val dataOffsets: Seq[Long] = {
    TransferDescriptor.dataOffsets(columns)
  }

  private[transfer] lazy val resultOffsets: Seq[Long] = {
    // Use columns from just one batch to avoid over-counting columns
    TransferDescriptor.resultOffsets(batches.headOption.getOrElse(Seq.empty[BytePointerColVector]))
  }

  lazy val buffer: BytePointer = {
    require(nbatches > 0, "Need more than 0 batches for transfer!")
    require(ncolumns > 0, "Need more than 0 columns for transfer!")
    require(batches.forall(_.size == ncolumns), "All batches must have the same column count!")
    logger.debug(s"Preparing transfer buffer for ${nbatches} batches of ${ncolumns} columns")

    // Total size of the buffer is computed from scan-left of the header and data sizes
    val tsize = dataOffsets.last

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

  def resultToColBatch(implicit source: VeColVectorSource): VeColBatch = {
    val vcolumns = batches.head.zipWithIndex.map { case (column, i) =>
      logger.debug(s"Reading output pointers for column ${i}")

      val batch = batchwiseColumns(i)
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
          Some(batch.map(_.dataSize).flatten.foldLeft(0)(_ + _))
      }

      VeColVector(
        source,
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
}

object BpcvTransferDescriptor {
  class Builder {
    val batches: ListBuffer[ListBuffer[BytePointerColVector]] = ListBuffer()
    var curBatch: Option[ListBuffer[BytePointerColVector]] = None

    def newBatch(): Builder = {
      val empty = ListBuffer[BytePointerColVector]()
      curBatch = Some(empty)
      batches += empty
      this
    }

    def addColumns(columns: Seq[BytePointerColVector]): Builder = {
      if (curBatch.isEmpty) newBatch()
      curBatch.get ++= columns
      this
    }

    def build(): BpcvTransferDescriptor = {
      val batchesList = batches.map(_.toList).toList
      if (batchesList.nonEmpty) {
        val size = batchesList.head.size
        require(batchesList.forall(_.size == size), "All batches must have the same column count!")
      }

      BpcvTransferDescriptor(batchesList)
    }
  }
}
