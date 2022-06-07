package com.nec.cache

import com.nec.colvector.{VeColBatch, VeColVector, VeColVectorSource}
import com.nec.spark.agile.SparkExpressionToCExpression
import com.nec.spark.agile.core._
import com.nec.util.FixedBitSet
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.bytedeco.javacpp.indexer.ByteRawIndexer
import org.bytedeco.javacpp.{BytePointer, LongPointer, Pointer}

case class InternalRowTransferDescriptor(colSchema: Seq[Attribute], rows: List[InternalRow])
  extends TransferDescriptor with LazyLogging {

  override def nonEmpty: Boolean = rows.nonEmpty

  private[cache] lazy val cols = colSchema.map{ col =>
    col -> SparkExpressionToCExpression.sparkTypeToVeType(col.dataType)
  }

  private[cache] lazy val colTypes = cols.map(_._2)

  private[cache] lazy val nbatches: Long = {
    1
  }

  private[cache] lazy val ncolumns: Long = {
    colSchema.size
  }

  private[cache] lazy val headerOffsets: Seq[Long] = {
    colTypes.map {
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

  /**
   * Hold on to already UTF-32 converted strings, if any
   */
  private[cache] lazy val stringCols: Map[Int, List[Array[Byte]]] = {
    colTypes.zipWithIndex
      .filter{ case (veType, _) => veType == VeString}
      .map { case (_, i) =>
        i -> rows.map{ row =>
          val UTF8String = row.getUTF8String(i)
          if(UTF8String == null){
            new Array[Byte](0)
          }else{
            UTF8String.toString.getBytes("UTF-32LE")
          }
        }
      }.toMap
  }

  private[cache] lazy val colSizes: Seq[Seq[Long]] = colTypes.zipWithIndex.map {
    case (c: VeScalarType, _) =>
      Seq(
        c.cSize * rows.size, // Data size
        (Math.ceil(rows.size / 64f) * 8).toLong // Validity Buffer Size
      )
    case (VeString, idx) =>
      Seq(
        stringCols(idx).map(_.length).sum, // Data size
        rows.size * 4, // Offsets size
        rows.size * 4, // Lengths size
        (Math.ceil(rows.size / 64f) * 8).toLong // Validity Buffer Size
      )
  }

  private[cache] lazy val dataOffsets: Seq[Seq[Long]] = {
    // TODO: Explain the "tail" and what this does in general
    colSizes
      // Get the size of each buffer in bytes
      .map { cs => cs.map(vectorAlignedSize) }
      // Start the accumulation from header total size
      // Offsets are in bytes
      .map { cs => cs.scanLeft(0L)(_ + _)}
      .scanLeft(Seq(headerOffsets.last * 8)){ (prev, cur) =>
        cur.map(_ + prev.last)
      }.tail
  }

  private[cache] lazy val resultOffsets: Seq[Long] = {
    colTypes.map {
        case _: VeScalarType =>
          // scalar vectors produce 3 pointers (struct, data buffer, validity buffer)
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
    logger.debug(s"Preparing transfer buffer for ${nbatches} batches of ${ncolumns} columns")

    // Total size of the buffer is computed from scan-left of the header and data sizes
    val tsize = (headerOffsets.last * 8) + dataOffsets.last.last

    logger.debug(s"Allocating transfer buffer of ${tsize} bytes")
    val outbuffer = new BytePointer(tsize)
    val header = new LongPointer(outbuffer)

    val outIndexer = new ByteRawIndexer(outbuffer)

    // Zero out the memory for consistency
    Pointer.memset(outbuffer, 0, outbuffer.limit)

    // Write the descriptor header
    // Total header size is in bytes
    header.put(0, headerOffsets.last * 8)
    header.put(1, nbatches)
    header.put(2, ncolumns)

    // Write the column headers
    colTypes.zipWithIndex.foreach { case (veType, i) =>
      val sizes = colSizes(i)
      val start = headerOffsets(i)

      header.put(start, veType.cEnumValue)
      header.put(start + 1, rows.size)
      header.put(start + 2, sizes(0))
      header.put(start + 3, sizes(1))

      if (veType.isString) {
        header.put(start + 4, sizes(2))
        header.put(start + 5, sizes(3))
      }
    }

    // Write the data from the individual column buffers
    colTypes.zipWithIndex.foreach{ case (veType, colIdx) =>
      val colDataOffsets = dataOffsets(colIdx)
      val colDataStart = colDataOffsets(0)

      val validityBuffer = FixedBitSet(rows.size)

      veType match {
        case VeString =>
          val colOffsetBufferStart = colDataOffsets(1)
          val colLengthsBufferStart = colDataOffsets(2)
          val colValidityBufferStart = colDataOffsets(3)

          val strings = stringCols(colIdx)
          var pos = 0
          for (elem <- strings) {
            outIndexer.put(colDataStart + pos, elem, 0, elem.length)
            pos += elem.length
          }

          val lengths = strings.map(_.length / 4)
          val offsets = lengths.scanLeft(0)(_+_)

          rows.zipWithIndex.foreach{ case (row, rowIdx) =>
            validityBuffer.set(rowIdx, !row.isNullAt(colIdx))
            outIndexer.putInt(colOffsetBufferStart + (rowIdx * 4), offsets(rowIdx))
            outIndexer.putInt(colLengthsBufferStart + (rowIdx * 4), lengths(rowIdx))
          }

          val validityBufferArray = validityBuffer.toByteArray
          outIndexer.put(colValidityBufferStart, validityBufferArray, 0, validityBufferArray.length)

        case veType: VeScalarType =>
          val colValidityBufferStart = colDataOffsets(1)

          rows.zipWithIndex.foreach{ case (row, rowIdx) =>
            if(!row.isNullAt(colIdx)){
              validityBuffer.set(rowIdx, true)
              val pos = colDataStart + (rowIdx * veType.cSize)
              veType match {
                case VeNullableDouble => outIndexer.putDouble(pos, row.getDouble(colIdx))
                case VeNullableFloat => outIndexer.putFloat(pos, row.getFloat(colIdx))
                case VeNullableShort => outIndexer.putInt(pos, row.getShort(colIdx))
                case VeNullableInt => outIndexer.putInt(pos, row.getInt(colIdx))
                case VeNullableLong => outIndexer.putLong(pos, row.getLong(colIdx))
              }
            }
          }

          val validityBufferArray = validityBuffer.toByteArray
          outIndexer.put(colValidityBufferStart, validityBufferArray, 0, validityBufferArray.length)
      }
    }

    outIndexer.close()
    outbuffer
  }

  lazy val resultBuffer: LongPointer = {
    require(nbatches > 0, "Need more than 0 batches for creating a result buffer!")
    require(ncolumns > 0, "Need more than 0 columns for creating a result buffer!")

    // Total size of the buffer is computed from scan-left of the result sizes
    logger.debug(s"Allocating transfer output pointer of ${resultOffsets.last} bytes")
    new LongPointer(resultOffsets.last)
  }

  def resultToColBatch(implicit source: VeColVectorSource): VeColBatch = {
    val vcolumns = cols.zipWithIndex.map { case ((column, veType), i) =>
      logger.debug(s"Reading output pointers for column ${i}")

      val cbuf = resultBuffer.position(resultOffsets(i))

      // Fetch the pointers to the nullable_t_vector
      val buffers = (if (veType.isString) 1.to(4) else 1.to(2))
        .toSeq
        .map(cbuf.get(_))

      // Compute the dataSize
      val dataSizeO = veType match {
        case _: VeScalarType =>
          None
        case VeString =>
          Some(stringCols(i).map(_.length).sum)
      }

      VeColVector(
        source,
        column.name,
        veType,
        rows.size,
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
