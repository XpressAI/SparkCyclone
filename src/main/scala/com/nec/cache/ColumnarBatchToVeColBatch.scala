package com.nec.cache

import com.nec.colvector.ArrowVectorConversions._
import com.nec.colvector.SparkSqlColumnVectorConversions._
import com.nec.colvector.{VeColBatch, VeColVectorSource}
import com.nec.util.CallContext
import com.nec.ve.{VeProcess, VeProcessMetrics}
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
    allocator: BufferAllocator,
    encoding: ArrowEncodingSettings,
    context: CallContext,
    veProcess: VeProcess,
    veColVectorSource: VeColVectorSource,
    cycloneMetrics: VeProcessMetrics,
  ): Iterator[VeColBatch] = {
    columnarBatches.map { columnarBatch =>
        (0 until columnarBatch.numCols())
          .map(i =>
            columnarBatch.column(i).getOptionalArrowValueVector match {
              case Some(acv) =>
                acv.toBytePointerColVector.asyncToVeColVector
              case None =>
                val field = arrowSchema.getFields.get(i)
                columnarBatch.column(i)
                  .toBytePointerColVector(field.getName, columnarBatch.numRows)
                  .asyncToVeColVector
            }
          )
    }.map{ it =>
      VeColBatch(it.map(_.apply()).map(_.get()).toList)
    }.toList.iterator
  }

  // TODO: This is currently dead code, maybe get rid of it (branch to call it is permanently disabled)
  def toVeColBatchesViaRows(
    columnarBatches: Iterator[ColumnarBatch],
    arrowSchema: Schema,
    completeInSpark: Boolean,
    metricsFn: (() => VeColBatch) => VeColBatch = (x) => { x() }
  )(implicit
    allocator: BufferAllocator,
    encoding: ArrowEncodingSettings,
    context: CallContext,
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
