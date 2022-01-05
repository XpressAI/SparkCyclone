package com.nec.spark.planning

import com.nec.spark.planning.VeCachedBatchSerializer.ArrowSerializedCachedBatch
import com.nec.ve.VeColBatch.{VeColVectorSource, VectorEngineLocation}
import com.nec.ve.{ByteBufferVeColVector, MaybeByteArrayColVector, VeColBatch, VeProcess}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, DualMode}
import org.apache.spark.{SparkContext, TaskContext}

import scala.collection.JavaConverters.asScalaBufferConverter

object VeColBatchConverters {

  def getNumRows(sparkContext: SparkContext, conf: SQLConf): Int = {
    sparkContext.getConf
      .getOption("com.nec.spark.ve.columnBatchSize")
      .map(_.toInt)
      .getOrElse(conf.columnBatchSize)
  }

  final case class ArrowColumnarBatch(columnarBatch: ColumnarBatch)

  final case class ByteArrayOrBufferColBatch(
    cols: Either[List[ByteBufferVeColVector], List[MaybeByteArrayColVector]]
  ) {
    def toVeColBatch()(implicit
      veProcess: VeProcess,
      veColVectorSource: VeColVectorSource
    ): VeColBatch = {
      import com.nec.ve.ByteBufferVeColVector._
      cols match {
        case Left(colsByteBuffer) =>
          VeColBatch.fromList(
            colsByteBuffer.map(
              _.transferBuffersToVe()
                .map(_.getOrElse(VectorEngineLocation(-1)))
                .newContainer()
            )
          )
        case Right(colsByteArray) =>
          import com.nec.ve.ByteArrayColVector._
          VeColBatch.fromList(
            colsByteArray.map(
              _.transferBuffersToVe()
                .map(_.getOrElse(VectorEngineLocation(-1)))
                .newContainer()
            )
          )
      }
    }
  }

  def internalRowIteratorToByteBufferVeColVector(
    rowIterator: Iterator[InternalRow],
    timeZoneId: String,
    schema: StructType,
    numRows: Int
  ): Iterator[List[ByteBufferVeColVector]] = {
    if (rowIterator.hasNext) {
      lazy implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
        .newChildAllocator(s"Writer for partial collector (Arrow)", 0, Long.MaxValue)
      new Iterator[List[ByteBufferVeColVector]] {
        private val arrowSchema = ArrowUtilsExposed.toArrowSchema(schema, timeZoneId)
        private val root = VectorSchemaRoot.create(arrowSchema, allocator)
        private val cb = new ColumnarBatch(
          root.getFieldVectors.asScala
            .map(vector => new ArrowColumnVector(vector))
            .toArray,
          root.getRowCount
        )

        private val arrowWriter = ArrowWriter.create(root)
        arrowWriter.finish()
        TaskContext.get().addTaskCompletionListener[Unit] { _ =>
          cb.close()
        }

        override def hasNext: Boolean = {
          rowIterator.hasNext
        }

        override def next(): List[ByteBufferVeColVector] = {
          arrowWriter.reset()
          cb.setNumRows(0)
          root.getFieldVectors.asScala.foreach(_.reset())
          var rowCount = 0
          while (rowCount < numRows && rowIterator.hasNext) {
            val row = rowIterator.next()
            arrowWriter.write(row)
            arrowWriter.finish()
            rowCount += 1
          }
          cb.setNumRows(rowCount)
          //              numInputRows += rowCount
          //              numOutputBatches += 1
          import com.nec.spark.SparkCycloneExecutorPlugin.source
          (0 until cb.numCols())
            .map(idx => ByteBufferVeColVector.fromVectorColumn(cb.column(idx)))
            .toList
        }
      }
    } else Iterator.empty
  }

  def internalRowToArrowSerializedBatch(
    input: RDD[InternalRow],
    timeZoneId: String,
    schema: StructType,
    numRows: Int
  ): RDD[CachedBatch] = input.mapPartitions { rowIterator: Iterator[InternalRow] =>
    internalRowIteratorToByteBufferVeColVector(rowIterator, timeZoneId, schema, numRows)
      .map { l =>
        import ByteBufferVeColVector.RichByteBufferVeColVector
        ArrowSerializedCachedBatch(numRows = l.head.numItems, cols = l.map(_.serializeBuffers()))
      }
  }

  def internalRowToSerializedBatch(
    input: RDD[InternalRow],
    timeZoneId: String,
    schema: StructType,
    numRows: Int
  ): RDD[ByteArrayOrBufferColBatch] = {
    input.mapPartitions { iterator =>
      DualMode.handleIterator(iterator) match {
        case Left(lvec) =>
          lvec.map(vl => ByteArrayOrBufferColBatch(Right(vl)))
        case Right(value) =>
          internalRowIteratorToByteBufferVeColVector(value, timeZoneId, schema, numRows)
            .map(vl => ByteArrayOrBufferColBatch(Left(vl)))
      }
    }
  }
}
