package com.nec.spark.planning

import com.nec.arrow.colvector.ByteBufferColVector
import com.nec.cache.DualColumnarBatchContainer
import com.nec.cache.VeColColumnarVector.CachedColumnVector
import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.spark.SparkCycloneExecutorPlugin.source
import com.nec.spark.planning.CEvaluationPlan.HasFieldVector.RichColumnVector
import com.nec.ve.VeColBatch
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
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

  object BasedOnColumnarBatch {
    object ColB {
      def unapply(internalRow: InternalRow): Option[VeColBatch] = ???
    }
  }

  type CachedColBatchWrapper = DualColumnarBatchContainer
  val CachedColBatchWrapper = DualColumnarBatchContainer

  def originalInternalRowToArrowColumnarBatches(
    rowIterator: Iterator[InternalRow],
    timeZoneId: String,
    schema: StructType,
    numRows: Int
  ): Iterator[ColumnarBatch] = {
    if (rowIterator.hasNext) {
      lazy implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
        .newChildAllocator(s"Writer for partial collector (Arrow)", 0, Long.MaxValue)
      new Iterator[ColumnarBatch] {
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

        override def next(): ColumnarBatch = {
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
          cb
        }
      }
    } else Iterator.empty
  }

  def internalRowToArrowSerializedColBatch(
    input: RDD[InternalRow],
    timeZoneId: String,
    schema: StructType,
    numRows: Int
  ): RDD[CachedColBatchWrapper] =
    input.mapPartitions { iterator =>
      DualMode.handleIterator(iterator) match {
        case Left(colBatches) =>
          colBatches.map(v => CachedColBatchWrapper(v))
        case Right(rowIterator) =>
          originalInternalRowToArrowColumnarBatches(
            rowIterator = rowIterator,
            timeZoneId = timeZoneId,
            schema = schema,
            numRows = numRows
          )
            .map { cb =>
              CachedColBatchWrapper(vecs = (0 until cb.numCols()).map { colNo =>
                Right(
                  ByteBufferColVector
                    .fromArrowVector(cb.column(colNo).getArrowValueVector)
                    .toByteArrayColVector()
                )
              }.toList)
            }
      }
    }

  def internalRowToVeColBatch(
    input: RDD[InternalRow],
    timeZoneId: String,
    schema: StructType,
    numRows: Int
  ): RDD[CachedColBatchWrapper] =
    input.mapPartitions { iterator =>
      DualMode.handleIterator(iterator) match {
        case Left(colBatches) =>
          colBatches.map(cachedColumnVectors => CachedColBatchWrapper(vecs = cachedColumnVectors))
        case Right(rowIterator) =>
          originalInternalRowToArrowColumnarBatches(
            rowIterator = rowIterator,
            timeZoneId = timeZoneId,
            schema = schema,
            numRows = numRows
          )
            .map { columnarBatch =>
              import SparkCycloneExecutorPlugin.veProcess
              val newBatch =
                try CachedColBatchWrapper(vecs =
                  VeColBatch.fromArrowColumnarBatch(columnarBatch).cols.map(cv => Left(cv))
                )
                finally columnarBatch.close()
              newBatch
            }
      }
    }

}
