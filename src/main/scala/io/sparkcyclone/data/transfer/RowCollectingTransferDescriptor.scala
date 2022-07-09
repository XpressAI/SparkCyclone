package io.sparkcyclone.data.transfer

import io.sparkcyclone.data.VeColVectorSource
import io.sparkcyclone.data.vector.{VeColBatch, VeColVector}
import io.sparkcyclone.spark.codegen.SparkExpressionToCExpression
import io.sparkcyclone.native.code._
import io.sparkcyclone.util.FixedBitSet
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.bytedeco.javacpp.indexer.ByteIndexer
import org.bytedeco.javacpp.{BytePointer, LongPointer, Pointer}

import scala.collection.mutable.ListBuffer

trait VeTransferCol {
  def veType: VeType
  def append(row: InternalRow): Unit
  def close(): Unit
  def headerSize: Int
  def totalDataSize: Long
  def resultSize: Int

  def writeHeader(target: LongPointer, targetPosition: Long): Long
  def writeData(target: BytePointer, targetPosition: Long): Long
}

case class VeScalarTransferCol(veType: VeScalarType, capacity: Int, idx: Int) extends VeTransferCol {
  private val pointer = new BytePointer(Pointer.calloc(capacity, veType.cSize)).capacity(capacity * veType.cSize)
  private val indexer = ByteIndexer.create(pointer)

  private val validityBuffer = FixedBitSet.ones(capacity)

  private var pos = 0
  private val appender = veType match {
    case VeNullableDouble => writer{ (row, pos) => indexer.putDouble(pos, row.getDouble(idx)) }
    case VeNullableFloat => writer{ (row, pos) => indexer.putFloat(pos, row.getFloat(idx)) }
    case VeNullableShort => writer{ (row, pos) => indexer.putInt(pos, row.getShort(idx)) }
    case VeNullableInt => writer{ (row, pos) => indexer.putInt(pos, row.getInt(idx)) }
    case VeNullableLong => writer{ (row, pos) => indexer.putLong(pos, row.getLong(idx)) }
  }

  def append(row: InternalRow): Unit = appender(row)

  private def writer(thunk: (InternalRow, Long) => Unit) = { row: InternalRow =>
    if(row.isNullAt(idx)){
      validityBuffer.clear(pos)
    }else{
      thunk(row, pos * veType.cSize)
    }
    pos += 1
  }

  def close(): Unit = {
    indexer.close()
    pointer.close()
  }

  private def count = pos
  private def dataSize = count * veType.cSize
  private def validityBufferSize = (count / 64f).ceil.toInt * 8

  def headerSize: Int = 4 * 8
  def writeHeader(target: LongPointer, targetPosition: Long): Long = {
    target.put(targetPosition, veType.cEnumValue)
    target.put(targetPosition + 1, count)
    target.put(targetPosition + 2, dataSize)
    target.put(targetPosition + 3, validityBufferSize)

    targetPosition + 4
  }

  def totalDataSize: Long = {
    Seq(dataSize, validityBufferSize)
      .map(x => TransferDescriptor.vectorAlignedSize(x.toLong))
      .sum
  }

  def writeData(target: BytePointer, targetPosition: Long): Long = {
    val curDataBytes = dataSize
    val curValidityBytes = validityBufferSize

    var pos = targetPosition

    val origPosition = target.position()
    target.position(targetPosition)
    Pointer.memcpy(target, pointer, curDataBytes)
    target.position(origPosition)

    pos += curDataBytes
    pos = TransferDescriptor.vectorAlignedSize(pos)

    target.position(pos).put(validityBuffer.toByteArray, 0, curValidityBytes).position(origPosition)

    TransferDescriptor.vectorAlignedSize(pos + curValidityBytes)
  }

  // scalar vectors produce 3 pointers (struct, data buffer, validity buffer)
  override def resultSize: Int = 3
}

case class VeStringTransferCol(idx: Int) extends VeTransferCol {
  val veType: VeType = VeString
  private val listBuffer: ListBuffer[Array[Byte]] = ListBuffer()
  private lazy val converted = listBuffer.toArray


  override def append(row: InternalRow): Unit = {
    val utf8String = row.getUTF8String(idx)
    listBuffer.append(if(utf8String == null){
      null
    }else{
      utf8String.toString.getBytes("UTF-32LE")
    })
  }

  override def close(): Unit = {}

  override def headerSize: Int = 6 * 8

  private def count = converted.length
  def dataSize: Int = converted.map{s => if(s == null) {0}else{s.length}}.sum
  private def offsetsSize = count * 4
  private def lengthsSize = count * 4
  private def validityBufferSize = (count / 64f).ceil.toInt * 8

  override def totalDataSize: Long = {
    Seq(dataSize, offsetsSize, lengthsSize, validityBufferSize)
      .map(x => TransferDescriptor.vectorAlignedSize(x.toLong))
      .sum
  }

  override def writeHeader(target: LongPointer, targetPosition: Long): Long = {
    target.put(targetPosition, veType.cEnumValue)
    target.put(targetPosition + 1, count)
    target.put(targetPosition + 2, dataSize)
    target.put(targetPosition + 3, offsetsSize)
    target.put(targetPosition + 4, lengthsSize)
    target.put(targetPosition + 5, validityBufferSize)

    targetPosition + 6
  }

  override def writeData(target: BytePointer, targetPosition: Long): Long = {
    val lengths = converted.map{s => if(s == null){ 0 }else{ s.length / 4 }}
    val offsets = lengths.scanLeft(0)(_+_).dropRight(1)

    val validityBuffer = FixedBitSet.ones(count)
    converted.zipWithIndex.filter(_._1 == null).foreach(t => validityBuffer.clear(t._2))

    val origPos = target.position()
    var pos = targetPosition
    converted.foreach{ str =>
      if(str != null){
        target.position(pos).put(str, 0, str.length)
        pos += str.length
      }
    }
    target.position(origPos)

    pos = TransferDescriptor.vectorAlignedSize(pos)
    offsets.foreach{ o =>
      target.putInt(pos, o)
      pos += 4
    }

    pos = TransferDescriptor.vectorAlignedSize(pos)
    lengths.foreach{ l =>
      target.putInt(pos, l)
      pos += 4
    }

    pos = TransferDescriptor.vectorAlignedSize(pos)
    val validityBufferArr = validityBuffer.toByteArray
    target.position(pos).put(validityBufferArr, 0, validityBufferArr.length).position(origPos)

    TransferDescriptor.vectorAlignedSize(pos + validityBufferArr.length)
  }

  // nullable_varchar_vector produce 5 pointers (struct, data buffer, offsets, lengths, validity buffer)
  override def resultSize: Int = 5
}

case class RowCollectingTransferDescriptor(schema: Seq[Attribute], capacity: Int) extends TransferDescriptor with LazyLogging {
  private var level = 0
  def nonFull: Boolean = level < capacity
  def isFull: Boolean = !nonFull
  override def nonEmpty: Boolean = level > 0

  private[transfer] val transferCols = schema.map(att => SparkExpressionToCExpression.sparkTypeToVeType(att.dataType)).zipWithIndex.map {
    case (t: VeScalarType, idx) => VeScalarTransferCol(t, capacity, idx)
    case (VeString, idx) => VeStringTransferCol(idx)
  }

  def append(row: InternalRow): Unit = {
    transferCols.foreach(_.append(row))
    level += 1
  }

  lazy val resultOffsets = transferCols.map(_.resultSize).scanLeft(0)(_+_)
  lazy val resultBuffer: LongPointer = {
    require(nonEmpty, "Need more than 0 rows for creating a result buffer!")

    // Total size of the buffer is computed from scan-left of the result sizes
    logger.debug(s"Allocating transfer output pointer of ${resultOffsets.last} bytes")
    new LongPointer(resultOffsets.last)
  }

  lazy val buffer: BytePointer = {
    val headerSize = 3 * 8 + transferCols.map(_.headerSize).sum
    val size = headerSize + transferCols.map(tCol => tCol.totalDataSize).sum

    val buffer = new BytePointer(Pointer.calloc(size, 1)).capacity(size)

    val header = new LongPointer(buffer)

    header.put(0, headerSize)
    header.put(1, 1)
    header.put(2, transferCols.size)

    var headerPos = 3L
    var dataPos = headerSize.toLong
    transferCols.foreach{ tCol =>
      headerPos = tCol.writeHeader(header, headerPos)
      dataPos = tCol.writeData(buffer, dataPos)
      tCol.close()
    }

    buffer
  }

  override def close: Unit = {
    resultBuffer.close()
    buffer.close()
  }

  override def resultToColBatch(implicit source: VeColVectorSource): VeColBatch = {
    val vcolumns = schema.zip(transferCols).zipWithIndex.map { case ((column, tCol), i) =>
      logger.debug(s"Reading output pointers for column ${i}")

      val veType = tCol.veType
      val cbuf = resultBuffer.position(resultOffsets(i))

      // Fetch the pointers to the nullable_t_vector
      val buffers = (if (veType.isString) 1.to(4) else 1.to(2))
        .toSeq
        .map(cbuf.get(_))

      // Compute the dataSize
      val dataSizeO = tCol match {
        case _: VeScalarTransferCol =>
          None
        case col: VeStringTransferCol =>
          Some(col.dataSize)
      }

      VeColVector(
        source,
        column.name,
        veType,
        level,
        buffers,
        dataSizeO,
        cbuf.get(0)
      )
    }

    VeColBatch(vcolumns)
  }
}
