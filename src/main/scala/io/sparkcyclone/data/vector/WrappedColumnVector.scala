package io.sparkcyclone.data.vector

import io.sparkcyclone.vectorengine.VeProcess
import org.apache.spark.sql.catalyst.expressions.{Attribute, PrettyAttribute}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.vectorized._
import org.apache.spark.unsafe.types.UTF8String

object WrappedColumnVector {
  sealed trait WrappedType
  case class VE(vector: VeColVector) extends WrappedType
  case class BP(vector: BytePointerColVector) extends WrappedType
  case class BA(vector: ByteArrayColVector) extends WrappedType

  def extractType(underlying: WrappedType): DataType = {
    underlying match {
      case VE(vec) => vec.veType.toSparkType
      case BP(vec) => vec.veType.toSparkType
      case BA(vec) => vec.veType.toSparkType
    }
  }

  def apply(vector: VeColVector): WrappedColumnVector = {
    WrappedColumnVector(VE(vector))
  }

  def apply(vector: BytePointerColVector): WrappedColumnVector = {
    WrappedColumnVector(BP(vector))
  }

  def apply(vector: ByteArrayColVector): WrappedColumnVector = {
    WrappedColumnVector(BA(vector))
  }
}

/*
  Placeholder class for a ColumnVector backed by a SparkCyclone-defined column
  vector.  This is mainly used for returning the output of Spark SQL cache
  deserialization, which requires a [[ColumnarBatch]] to be returned.  As of
  Spark 3.1.3, [[ColumnarBatch]] is defined with the `final` keyword, so
  [[ColumnVector]] is extended as a workaround.

  This is NOT for general consumption. The `unsupported` calls are intentional.
  If you are being led here, there is something off in the planning stages.
*/
final case class WrappedColumnVector(val underlying: WrappedColumnVector.WrappedType)
  extends ColumnVector(WrappedColumnVector.extractType(underlying)) {

  def name: String = {
    underlying match {
      case WrappedColumnVector.VE(vec) => vec.name
      case WrappedColumnVector.BP(vec) => vec.name
      case WrappedColumnVector.BA(vec) => vec.name
    }
  }

  def attribute: Attribute = {
    PrettyAttribute(name, dataType)
  }

  def toVeColVector(implicit process: VeProcess): VeColVector = {
    underlying match {
      case WrappedColumnVector.VE(vec) => vec
      case WrappedColumnVector.BP(vec) => vec.toVeColVector
      case WrappedColumnVector.BA(vec) => vec.toBytePointerColVector.toVeColVector
    }
  }

  private[vector] def unsupported: Nothing = {
    throw new UnsupportedOperationException("Operation is not supported - this class is only intended as a carrier class.")
  }

  override def close: Unit = {}

  override def hasNull: Boolean = unsupported

  override def numNulls: Int = unsupported

  override def isNullAt(rowId: Int): Boolean = unsupported

  override def getBoolean(rowId: Int): Boolean = unsupported

  override def getByte(rowId: Int): Byte = unsupported

  override def getShort(rowId: Int): Short = unsupported

  override def getInt(rowId: Int): Int = unsupported

  override def getLong(rowId: Int): Long = unsupported

  override def getFloat(rowId: Int): Float = unsupported

  override def getDouble(rowId: Int): Double = unsupported

  override def getArray(rowId: Int): ColumnarArray = unsupported

  override def getMap(ordinal: Int): ColumnarMap = unsupported

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal = unsupported

  override def getUTF8String(rowId: Int): UTF8String = unsupported

  override def getBinary(rowId: Int): Array[Byte] = unsupported

  override def getChild(ordinal: Int): ColumnVector = unsupported
}
