package com.nec.spark.planning

import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.spark.SparkCycloneExecutorPlugin.source
import com.nec.spark.planning.VeCachedBatchSerializer.ArrowSerializedCachedBatch
import com.nec.ve.VeColBatch.VectorEngineLocation
import com.nec.ve.{ByteBufferVeColVector, VeColBatch}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}
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

  final case class UnInternalVeColBatch(veColBatch: VeColBatch)
  final case class ByteBufferColBatch(numRows: Int, cols: List[ByteBufferVeColVector])

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

  def internalRowToVeColBatch(
    input: RDD[InternalRow],
    timeZoneId: String,
    schema: StructType,
    numRows: Int
  ): RDD[UnInternalVeColBatch] = {
    input.mapPartitions { iterator =>
      internalRowIteratorToByteBufferVeColVector(iterator, timeZoneId, schema, numRows)
        .map { lvec =>
          import ByteBufferVeColVector.RichByteBufferVeColVector
          import SparkCycloneExecutorPlugin.veProcess
          val newBatch =
            UnInternalVeColBatch(veColBatch =
              VeColBatch.fromList(
                lvec.map(
                  _.transferBuffersToVe()
                    .map(_.getOrElse(VectorEngineLocation(-1)))
                    .newContainer()
                )
              )
            )
          SparkCycloneExecutorPlugin.register(newBatch.veColBatch)
          newBatch
        }
    }
  }
}
