package com.nec.spark.planning

import com.nec.ve.VeColBatch.VeColVector
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarArray, ColumnarMap}
import org.apache.spark.unsafe.types.UTF8String

final class VeColColumnarVector(val veColVector: VeColVector, dataType: DataType)
  extends ColumnVector(dataType) {
  import com.nec.spark.SparkCycloneExecutorPlugin.veProcess

  override def close(): Unit = veColVector.free()

  override def hasNull: Boolean = ???

  override def numNulls(): Int = ???

  override def isNullAt(rowId: Int): Boolean = ???

  override def getBoolean(rowId: Int): Boolean = ???

  override def getByte(rowId: Int): Byte = ???

  override def getShort(rowId: Int): Short = ???

  override def getInt(rowId: Int): Int = ???

  override def getLong(rowId: Int): Long = ???

  override def getFloat(rowId: Int): Float = ???

  override def getDouble(rowId: Int): Double = ???

  override def getArray(rowId: Int): ColumnarArray = ???

  override def getMap(ordinal: Int): ColumnarMap = ???

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal = ???

  override def getUTF8String(rowId: Int): UTF8String = ???

  override def getBinary(rowId: Int): Array[Byte] = ???

  override def getChild(ordinal: Int): ColumnVector = ???
}
