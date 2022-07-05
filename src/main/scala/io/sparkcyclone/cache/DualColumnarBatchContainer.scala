package io.sparkcyclone.cache

import io.sparkcyclone.colvector.ArrowVectorConversions._
import io.sparkcyclone.colvector._
import io.sparkcyclone.util.CallContext
import io.sparkcyclone.ve.VeProcessMetrics
import io.sparkcyclone.vectorengine.VeProcess
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

/**
 * This wraps the two possibilities of caches - either on the VE memory, or in Spark Arrow serialization form
 */
final case class DualColumnarBatchContainer(vecs: List[Either[VeColVector, ByteArrayColVector]]) {

  def toEither: Either[VeColBatch, ByteArrayColBatch] = {
    Option(vecs.flatMap(_.left.toSeq)).filter(_.nonEmpty) match {
      case Some(vecs) => Left(VeColBatch(vecs))
      case None =>
        val cols = vecs.flatMap(_.right.toSeq)
        Right(ByteArrayColBatch(cols))
    }
  }

  def toArrowColumnarBatch()(implicit
    allocator: BufferAllocator,
    veProcess: VeProcess
  ): ColumnarBatch = {
    Option(vecs.flatMap(_.left.toSeq)).filter(_.nonEmpty) match {
      case Some(vecs) => VeColBatch(vecs).toArrowColumnarBatch
      case None =>
        val byteArrayColVectors = vecs.flatMap(_.right.toSeq)

        val vecsx =
          byteArrayColVectors.map(_.toBytePointerColVector.toArrowVector)
        val cb = new ColumnarBatch(vecsx.map(col => new ArrowColumnVector(col)).toArray)
        cb.setNumRows(vecs.head.fold(_.numItems, _.numItems))
        cb
    }
  }

  def toVEColBatch()(implicit
    veProcess: VeProcess,
    veColVectorSource: VeColVectorSource,
    context: CallContext,
    cycloneMetrics: VeProcessMetrics
  ): VeColBatch = {
    Option(vecs.flatMap(_.left.toSeq)).filter(_.nonEmpty) match {
      case Some(vecs) => VeColBatch(vecs)
      case None =>
        val byteArrayColVectors = vecs.flatMap(_.right.toSeq)
        VeColBatch(byteArrayColVectors.map(_.asyncToVeColVector).map(_.apply()).map(_.get))
    }
  }

  def toInternalColumnarBatch(): ColumnarBatch = {
    Option(vecs.flatMap(_.left.toSeq)).filter(_.nonEmpty) match {
      case Some(vec) => VeColBatch(vec).toSparkColumnarBatch
      case None =>
        val byteArrayColVectors = vecs.flatMap(_.right.toSeq)
        val veColColumnarVectors = byteArrayColVectors.map(bar =>
          new VeColColumnarVector(Right(bar), bar.veType.toSparkType)
        )
        val columnarBatch = new ColumnarBatch(veColColumnarVectors.toArray)
        columnarBatch.setNumRows(byteArrayColVectors.head.numItems)
        columnarBatch
    }
  }

  def numRows: Int = vecs.head.fold(_.numItems, _.numItems)

  def onCpuSize: Option[Long] = {
    Option(vecs.flatMap(_.right.toSeq))
    .filter(_.nonEmpty)
    .map(_.map(_.buffers.map(_.size).sum).sum.toLong)
  }
}
