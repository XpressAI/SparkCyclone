package com.nec.cache

import com.nec.arrow.colvector.ByteArrayColVector
import com.nec.cache.VeColColumnarVector.CachedColumnVector
import com.nec.ve.VeColBatch.VeColVector
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.vectorized._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Placeholder class for a ColumnVector backed by VeColVector.
 *
 * The get* methods are *not* supposed to be accessed by Spark, but rather be a carrier of
 * [[dualVeBatch]] which we extract. Specifically used by the caching/serialization mechanism here.
 *
 * This is NOT for general consumption. The `unsupported` calls are intentional.
 * If you are being led here, there is something off in the planning stages.
 */

object VeColColumnarVector {
  def unsupported(): Nothing = throw new UnsupportedOperationException(
    "Operation is not supported - this class is only intended as a carrier class."
  )
  type CachedColumnVector = Either[VeColVector, ByteArrayColVector]
}

final class VeColColumnarVector(val dualVeBatch: CachedColumnVector, dataType: DataType)
  extends ColumnVector(dataType) {

  override def close(): Unit = {

    //dualVeBatch.left.foreach(SparkCycloneExecutorPlugin.freeCachedCol)
  }

  override def hasNull: Boolean = VeColColumnarVector.unsupported()

  override def numNulls(): Int = VeColColumnarVector.unsupported()

  override def isNullAt(rowId: Int): Boolean = VeColColumnarVector.unsupported()

  override def getBoolean(rowId: Int): Boolean = VeColColumnarVector.unsupported()

  override def getByte(rowId: Int): Byte = VeColColumnarVector.unsupported()

  override def getShort(rowId: Int): Short = VeColColumnarVector.unsupported()

  override def getInt(rowId: Int): Int = VeColColumnarVector.unsupported()

  override def getLong(rowId: Int): Long = VeColColumnarVector.unsupported()

  override def getFloat(rowId: Int): Float = VeColColumnarVector.unsupported()

  override def getDouble(rowId: Int): Double = VeColColumnarVector.unsupported()

  override def getArray(rowId: Int): ColumnarArray = VeColColumnarVector.unsupported()

  override def getMap(ordinal: Int): ColumnarMap = VeColColumnarVector.unsupported()

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal =
    VeColColumnarVector.unsupported()

  override def getUTF8String(rowId: Int): UTF8String = VeColColumnarVector.unsupported()

  override def getBinary(rowId: Int): Array[Byte] = VeColColumnarVector.unsupported()

  override def getChild(ordinal: Int): ColumnVector = VeColColumnarVector.unsupported()
}
