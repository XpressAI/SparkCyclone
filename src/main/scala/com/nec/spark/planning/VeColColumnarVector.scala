package com.nec.spark.planning

import com.nec.arrow.colvector.ByteArrayColVector
import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.spark.agile.SparkExpressionToCExpression.likelySparkType
import com.nec.spark.planning.VeColColumnarVector.CachedColVector
import com.nec.ve.{VeColBatch, VeProcess}
import com.nec.ve.VeColBatch.VeColVector
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.vectorized.{
  ArrowColumnVector,
  ColumnVector,
  ColumnarArray,
  ColumnarBatch,
  ColumnarMap
}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Placeholder class for a ColumnVector backed by VeColVector.
 *
 * The get* methods are *not* supposed to be accessed by Spark, but rather be a carrier of
 * [[dualVeBatch]] which we extract. Specifically used by the caching/serialization mechanism here.
 */
final class VeColColumnarVector(val dualVeBatch: CachedColVector, dataType: DataType)
  extends ColumnVector(dataType) {
  import com.nec.spark.SparkCycloneExecutorPlugin.veProcess

  override def close(): Unit = dualVeBatch.left.foreach(SparkCycloneExecutorPlugin.freeCol)

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

object VeColColumnarVector {
  def unsupported(): Nothing = throw new UnsupportedOperationException(
    "Operation is not supported - this class is only intended as a carrier class."
  )

  type CachedColVector = Either[VeColVector, ByteArrayColVector]
  final case class DualVeBatch(vecs: List[CachedColVector]) {
    def toArrowColumnarBatch()(implicit
      bufferAllocator: BufferAllocator,
      veProcess: VeProcess
    ): ColumnarBatch =
      Option(vecs.flatMap(_.left.toSeq)).filter(_.nonEmpty) match {
        case Some(vecs) => VeColBatch.fromList(vecs).toArrowColumnarBatch()
        case None =>
          val byteArrayColVectors = vecs.flatMap(_.right.toSeq)

          val vecsx =
            byteArrayColVectors.map(_.transferToByteBuffers().toArrowVector())
          val cb = new ColumnarBatch(vecsx.map(col => new ArrowColumnVector(col)).toArray)
          cb.setNumRows(vecs.head.fold(_.numItems, _.underlying.numItems))
          cb
      }

    def toVEColBatch()(implicit
      veProcess: VeProcess,
      veColVectorSource: VeColVectorSource
    ): VeColBatch = {
      Option(vecs.flatMap(_.left.toSeq)).filter(_.nonEmpty) match {
        case Some(vecs) => VeColBatch.fromList(vecs)
        case None =>
          val byteArrayColVectors = vecs.flatMap(_.right.toSeq)
          VeColBatch.fromList(byteArrayColVectors.map(_.transferToByteBuffers().toVeColVector()))
      }
    }

    def toInternalColumnarBatch(): ColumnarBatch = {
      Option(vecs.flatMap(_.left.toSeq)).filter(_.nonEmpty) match {
        case Some(vec) => VeColBatch.fromList(vec).toInternalColumnarBatch()
        case None =>
          val byteArrayColVectors = vecs.flatMap(_.right.toSeq)
          val veColColumnarVectors = byteArrayColVectors.map(bar =>
            new VeColColumnarVector(Right(bar), likelySparkType(bar.underlying.veType))
          )
          val columnarBatch = new ColumnarBatch(veColColumnarVectors.toArray)
          columnarBatch.setNumRows(byteArrayColVectors.head.underlying.numItems)
          columnarBatch
      }
    }

    def numRows: Int = vecs.head.fold(_.numItems, _.underlying.numItems)
    def onCpuSize: Option[Long] = Option(vecs.flatMap(_.right.toSeq))
      .filter(_.nonEmpty)
      .map(_.map(_.underlying.bufferSizes.sum).sum.toLong)

  }
}
