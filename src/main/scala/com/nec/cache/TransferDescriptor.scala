package com.nec.cache

import com.nec.colvector.{BytePointerColVector, VeColBatch, VeColVector}
import com.typesafe.scalalogging.LazyLogging
import org.bytedeco.javacpp.{BytePointer, LongPointer, Pointer}

import scala.collection.mutable.ListBuffer

case class TransferDescriptor(
  batches: List[List[BytePointerColVector]]) extends LazyLogging {
  lazy val isEmpty: Boolean = batches.flatten.isEmpty
  def nonEmpty: Boolean = !isEmpty

  def closeTransferBuffer(): Unit = buffer.close()
  lazy val buffer: BytePointer = {
    require(nonEmpty, "Can not create transfer buffer for empty TransferDescriptor!")

    val sizeOfSizeT = 8

    // Columns are arranged such that the first column of all batches comes first, then the second one of all batches
    val columns = batches.transpose.flatten

    // 3 size_t fields: [header_size, batch_count, column_count]
    val transferHeaderFieldCount = 3
    val startingPositions = columns.map(_.veType.isString).map{
      case true => 1 + 5 // [column_type][element_count][data_size][offsets_size][lengths_size][validity_buffer_size]
      case false => 1 + 3 // [column_type][element_count][data_size][validity_buffer_size]
    }.foldLeft(ListBuffer(transferHeaderFieldCount)){ case (acc, size) =>
      acc += acc.last + size
    }.toList
    // As startingPositions starts after the header, its cum-sum reflects the total number of elements in the header
    val totalHeaderSize: Long = startingPositions.last * sizeOfSizeT
    val batchCount: Long = batches.size
    val columnCount: Long = batches.head.size

    val dataSize = batches.flatten.flatMap(_.buffers).map(_.limit()).sum

    val totalBufferSize = totalHeaderSize + dataSize

    logger.debug(s"Allocating transfer buffer of $totalBufferSize bytes")
    val buffer = new BytePointer(totalBufferSize)

    logger.debug(s"Writing header")
    // Setup header values in buffer
    val header = new LongPointer(buffer)
    header.put(0, totalHeaderSize)
    header.put(1, batchCount)
    header.put(2, columnCount)

    columns.zipWithIndex.foreach{ case (column, idx) =>
      val buffers = column.buffers.toList

      val startPos = startingPositions(idx)

      val columnType: Long = column.veType.cVectorType match {
        case "nullable_short_vector" => 0
        case "nullable_int_vector" => 1
        case "nullable_bigint_vector" => 2
        case "nullable_float_vector" => 3
        case "nullable_double_vector" => 4
        case "nullable_varchar_vector" => 5
      }

      logger.debug(s"Writing column header for column $idx")

      header.put(startPos, columnType)
      header.put(startPos + 1, column.numItems)
      header.put(startPos + 2, buffers(0).limit())
      if(column.veType.isString){
        header.put(startPos + 3, buffers(1).limit())
        header.put(startPos + 4, buffers(2).limit())
        header.put(startPos + 5, buffers(3).limit())
      }else{
        header.put(startPos + 3, buffers(1).limit())
      }
    }

    // copy data into buffer
    val bufferPointers = columns.flatMap(_.buffers)
    val dataPositions = bufferPointers.map(_.limit())
      .foldLeft(ListBuffer(totalHeaderSize)){ case (acc, size) =>
        acc += acc.last + size
      }

    bufferPointers.zipWithIndex.foreach{ case (ptr, idx) =>
      val startPos = dataPositions(idx)

      logger.debug(s"Copying data for buffer $idx")
      Pointer.memcpy(buffer.position(startPos), ptr, ptr.limit())
    }

    buffer.position(0)
  }

  def closeOutputBuffer(): Unit = outputBuffer.close()
  lazy val outputBuffer: BytePointer = {
    require(nonEmpty, "Can not create output buffer for empty TransferDescriptor!")

    val sizeOfUIntPtr = 8
    val bufferSize = batches.head.map(_.veType.isString).map{
      // varchar vectors produce 5 pointers (pointer to structure + data buffer + offsets + lengths + validity buffer)
      case true => 5
      // scalar vectors prodduce 3 pointers (pointer to structure + data buffer + validity buffer)
      case false => 3
    }.sum * sizeOfUIntPtr

    logger.debug(s"Allocating transfer output pointer of $bufferSize bytes")
    new BytePointer(bufferSize)
  }
  def outputBufferToColBatch(): VeColBatch = {
    val od = new LongPointer(outputBuffer)
    val startingPositions = batches.head.map(_.veType.isString).map{
      case true => 4
      case false => 3
    }.foldLeft(ListBuffer(0)){ case (acc, size) =>
      acc += acc.last + size
    }

    val batchwiseColumns = batches.transpose

    val vectors = batches.head.zipWithIndex.map{ case (column, idx) =>
      logger.debug(s"Reading output pointers for column $idx")
      val curOd = od.position(startingPositions(idx))
      val buffers = if (column.veType.isString) {
        Seq(
          curOd.get(1), curOd.get(2), curOd.get(3), curOd.get(4),
        )
      } else {
        Seq(
          curOd.get(1), curOd.get(2)
        )
      }

      val batch = batchwiseColumns(idx)
      val items = batch.map(_.numItems).sum
      val dataSize = batch.map(_.dataSize).foldLeft(Option(0)){ case (acc, v) =>
        v match {
          case None => acc
          case Some(value) => acc.map(_ + value)
        }
      }

      VeColVector(
        column.source,
        column.name,
        column.veType,
        items,
        buffers,
        dataSize,
        curOd.get(0)
      )
    }

    VeColBatch(vectors)
  }
}

object TransferDescriptor {
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
      if(curBatch.isEmpty) newBatch()
      curBatch.get ++= columns
      this
    }

    def build(): TransferDescriptor = {
      val batchesList = batches.map(_.toList).toList
      if(batchesList.nonEmpty){
        val size = batchesList.head.size
        require(batchesList.forall(_.size == size), "All batches must have the same column count!")
      }

      TransferDescriptor(batchesList)
    }
  }
}
