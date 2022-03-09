package com.nec.cache

import com.nec.arrow.ArrowEncodingSettings
import com.nec.arrow.colvector.BytePointerColVector
import com.nec.spark.planning.CEvaluationPlan.HasFieldVector.RichColumnVector
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.{VeColVector, VeColVectorSource}
import com.nec.ve.{VeColBatch, VeProcess, VeProcessMetrics}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.vectorized.ColumnarBatch

object ColumnarBatchToVeColBatch {
  def toVeColBatchesViaCols(
    columnarBatches: Iterator[ColumnarBatch],
    arrowSchema: Schema,
    completeInSpark: Boolean,
    metricsFn: (() => VeColBatch) => VeColBatch = (x) => { x() }
  )(implicit
    bufferAllocator: BufferAllocator,
    arrowEncodingSettings: ArrowEncodingSettings,
    originalCallingContext: OriginalCallingContext,
    veProcess: VeProcess,
    veColVectorSource: VeColVectorSource,
    cycloneMetrics: VeProcessMetrics,
  ): Iterator[VeColBatch] = {
    columnarBatches.map { columnarBatch =>
      metricsFn { () =>
        VeColBatch.fromList(
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
            ).toList
        )
      }
    }
  }

  def toVeColBatchesViaRows(
    columnarBatches: Iterator[ColumnarBatch],
    arrowSchema: Schema,
    completeInSpark: Boolean,
    metricsFn: (() => VeColBatch) => VeColBatch = (x) => { x() }
  )(implicit
    bufferAllocator: BufferAllocator,
    arrowEncodingSettings: ArrowEncodingSettings,
    originalCallingContext: OriginalCallingContext,
    veProcess: VeProcess,
    veColVectorSource: VeColVectorSource,
    cycloneMetrics: VeProcessMetrics
  ): Iterator[VeColBatch] = {
    columnarBatches.flatMap { columnarBatch =>
      import scala.collection.JavaConverters._
        SparkInternalRowsToArrowColumnarBatches
          .apply(
            rowIterator = columnarBatch.rowIterator().asScala,
            arrowSchema = arrowSchema,
            completeInSpark = completeInSpark
          )
          .map { columnarBatch =>
            metricsFn  { () =>
              /* cleaning up the [[columnarBatch]] is not necessary as the underlying ones does it */
              VeColBatch.fromArrowColumnarBatch(columnarBatch)
            }
        }
    }
  }

}
