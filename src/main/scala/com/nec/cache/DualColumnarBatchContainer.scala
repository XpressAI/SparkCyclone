package com.nec.cache

import com.nec.colvector.ArrowVectorConversions._
import com.nec.colvector.{ByteArrayColBatch, ByteArrayColVector, GenericColBatch, VeColVector}
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.colvector.VeColBatch.VeColVectorSource
import com.nec.ve.{VeColBatch, VeProcess, VeProcessMetrics}
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

/**
 * This wraps the two possibilities of caches - either on the VE memory, or in Spark Arrow serialization form
 */
final case class DualColumnarBatchContainer(vecs: List[Either[VeColVector, ByteArrayColVector]]) {

  def toEither: Either[VeColBatch, ByteArrayColBatch] = {
    Option(vecs.flatMap(_.left.toSeq)).filter(_.nonEmpty) match {
      case Some(vecs) => Left(VeColBatch.fromList(vecs))
      case None =>
        val cols = vecs.flatMap(_.right.toSeq)
        Right(
          ByteArrayColBatch(GenericColBatch(numRows = cols.head.numItems, cols = cols))
        )
    }
  }

  def toArrowColumnarBatch()(implicit
    bufferAllocator: BufferAllocator,
    veProcess: VeProcess
  ): ColumnarBatch = {
    Option(vecs.flatMap(_.left.toSeq)).filter(_.nonEmpty) match {
      case Some(vecs) => VeColBatch.fromList(vecs).toArrowColumnarBatch()
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
    originalCallingContext: OriginalCallingContext,
    cycloneMetrics: VeProcessMetrics
  ): VeColBatch = {
    Option(vecs.flatMap(_.left.toSeq)).filter(_.nonEmpty) match {
      case Some(vecs) => VeColBatch.fromList(vecs)
      case None =>
        val byteArrayColVectors = vecs.flatMap(_.right.toSeq)
        VeColBatch.fromList(byteArrayColVectors.map(_.toVeColVector))
    }
  }

  def toInternalColumnarBatch(): ColumnarBatch = {
    Option(vecs.flatMap(_.left.toSeq)).filter(_.nonEmpty) match {
      case Some(vec) => VeColBatch.fromList(vec).toInternalColumnarBatch()
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
