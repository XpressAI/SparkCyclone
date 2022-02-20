package com.nec.cache

import com.nec.arrow.ArrowEncodingSettings
import com.nec.ve.{VeColBatch, VeProcess}
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.vectorized.ColumnarBatch

object ColumnarBatchToVeColBatch {
  def toVeColBatchesViaCols(
    columnarBatches: Iterator[ColumnarBatch],
    arrowSchema: Schema,
    completeInSpark: Boolean
  )(implicit
    bufferAllocator: BufferAllocator,
    arrowEncodingSettings: ArrowEncodingSettings,
    originalCallingContext: OriginalCallingContext,
    veProcess: VeProcess,
    veColVectorSource: VeColVectorSource
  ): Iterator[VeColBatch] = {
    ???
  }

  def toVeColBatchesViaRows(
    columnarBatches: Iterator[ColumnarBatch],
    arrowSchema: Schema,
    completeInSpark: Boolean
  )(implicit
    bufferAllocator: BufferAllocator,
    arrowEncodingSettings: ArrowEncodingSettings,
    originalCallingContext: OriginalCallingContext,
    veProcess: VeProcess,
    veColVectorSource: VeColVectorSource
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
          /* cleaning up the [[columnarBatch]] is not necessary as the underlying ones does it */
          VeColBatch.fromArrowColumnarBatch(columnarBatch)
        }
    }
  }
}
