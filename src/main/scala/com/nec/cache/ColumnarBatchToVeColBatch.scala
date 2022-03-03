package com.nec.cache

import com.nec.arrow.ArrowEncodingSettings
import com.nec.arrow.colvector.BytePointerColVector
import com.nec.spark.planning.CEvaluationPlan.HasFieldVector.RichColumnVector
import com.nec.ve.{VeColBatch, VeProcess, VeProcessMetrics}
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.{VeColVector, VeColVectorSource}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

object ColumnarBatchToVeColBatch {
  def toVeColBatchesViaCols(
    columnarBatches: Iterator[ColumnarBatch],
    arrowSchema: Schema,
    completeInSpark: Boolean,
    inputBatchRows: SQLMetric,
    inputBatchCols: SQLMetric,
    outputBatchRows: SQLMetric,
    outputBatchCols: SQLMetric
  )(implicit
    bufferAllocator: BufferAllocator,
    arrowEncodingSettings: ArrowEncodingSettings,
    originalCallingContext: OriginalCallingContext,
    veProcess: VeProcess,
    veColVectorSource: VeColVectorSource,
    cycloneMetrics: VeProcessMetrics
  ): Iterator[VeColBatch] = {
    columnarBatches.map { columnarBatch =>
      inputBatchRows.set(columnarBatch.numRows())
      inputBatchCols.set(columnarBatch.numCols())

      val res = VeColBatch.fromList(
        (0 until columnarBatch.numCols())
          .map(i =>
            columnarBatch.column(i).getOptionalArrowValueVector match {
              case Some(acv) =>
                VeColVector.fromArrowVector(acv)
              case None =>
                val field = arrowSchema.getFields.get(i)
                BytePointerColVector
                  .fromColumnarVectorViaArrow(
                    field.getName,
                    columnarBatch.column(i),
                    columnarBatch.numRows()
                  ) match {
                  case None =>
                    throw new NotImplementedError(
                      s"Type ${columnarBatch.column(i).dataType()} not supported for columnar batch conversion"
                    )
                  case Some((fieldVector, bytePointerColVector)) =>
                    try bytePointerColVector.toVeColVector()
                    finally fieldVector.close()
                }
            }
          )
          .toList
      )

      outputBatchRows.set(res.numRows)
      outputBatchCols.set(res.cols.size)

      res
    }
  }

  def toVeColBatchesViaRows(
    columnarBatches: Iterator[ColumnarBatch],
    arrowSchema: Schema,
    completeInSpark: Boolean,
    inputBatchRows: SQLMetric,
    inputBatchCols: SQLMetric,
    outputBatchRows: SQLMetric,
    outputBatchCols: SQLMetric
  )(implicit
    bufferAllocator: BufferAllocator,
    arrowEncodingSettings: ArrowEncodingSettings,
    originalCallingContext: OriginalCallingContext,
    veProcess: VeProcess,
    veColVectorSource: VeColVectorSource,
    cycloneMetrics: VeProcessMetrics
  ): Iterator[VeColBatch] = {
    columnarBatches.flatMap { columnarBatch =>
      inputBatchRows.set(columnarBatch.numRows())
      inputBatchCols.set(columnarBatch.numCols())

      import scala.collection.JavaConverters._
      SparkInternalRowsToArrowColumnarBatches
        .apply(
          rowIterator = columnarBatch.rowIterator().asScala,
          arrowSchema = arrowSchema,
          completeInSpark = completeInSpark
        )
        .map { columnarBatch =>
          /* cleaning up the [[columnarBatch]] is not necessary as the underlying ones does it */
          val res = VeColBatch.fromArrowColumnarBatch(columnarBatch)

          outputBatchRows.set(res.numRows)
          outputBatchCols.set(res.cols.size)

          res
        }
    }
  }

}
