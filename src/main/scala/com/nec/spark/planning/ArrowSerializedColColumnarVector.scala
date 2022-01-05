package com.nec.spark.planning

import com.nec.ve.MaybeByteArrayColVector
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarArray, ColumnarMap}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Placeholder class for a ColumnVector backed by VeColVector.
 *
 * The get* methods are *not* supposed to be accessed by Spark, but rather be a carrier of
 * [[veColVector]] which we extract. Specifically used by the caching/serialization mechanism here.
 */
final class ArrowSerializedColColumnarVector(
  val veColVector: MaybeByteArrayColVector,
  dataType: DataType
) extends ColumnVector(dataType) {

  override def close(): Unit = ()

  override def hasNull: Boolean = ArrowSerializedColColumnarVector.unsupported()

  override def numNulls(): Int = ArrowSerializedColColumnarVector.unsupported()

  override def isNullAt(rowId: Int): Boolean = ArrowSerializedColColumnarVector.unsupported()

  override def getBoolean(rowId: Int): Boolean = ArrowSerializedColColumnarVector.unsupported()

  override def getByte(rowId: Int): Byte = ArrowSerializedColColumnarVector.unsupported()

  override def getShort(rowId: Int): Short = ArrowSerializedColColumnarVector.unsupported()

  override def getInt(rowId: Int): Int = ArrowSerializedColColumnarVector.unsupported()

  override def getLong(rowId: Int): Long = ArrowSerializedColColumnarVector.unsupported()

  override def getFloat(rowId: Int): Float = ArrowSerializedColColumnarVector.unsupported()

  override def getDouble(rowId: Int): Double = ArrowSerializedColColumnarVector.unsupported()

  override def getArray(rowId: Int): ColumnarArray = ArrowSerializedColColumnarVector.unsupported()

  override def getMap(ordinal: Int): ColumnarMap = ArrowSerializedColColumnarVector.unsupported()

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal =
    ArrowSerializedColColumnarVector.unsupported()

  override def getUTF8String(rowId: Int): UTF8String =
    ArrowSerializedColColumnarVector.unsupported()

  override def getBinary(rowId: Int): Array[Byte] = ArrowSerializedColColumnarVector.unsupported()

  override def getChild(ordinal: Int): ColumnVector = ArrowSerializedColColumnarVector.unsupported()
}

object ArrowSerializedColColumnarVector {
  def unsupported(): Nothing = throw new UnsupportedOperationException(
    "Operation is not supported - this class is only intended as a carrier class."
  )
}
