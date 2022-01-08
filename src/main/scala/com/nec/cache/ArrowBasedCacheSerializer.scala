package com.nec.cache

import com.nec.arrow.ArrowEncodingSettings
import com.nec.arrow.colvector.ByteBufferColVector
import com.nec.spark.planning.CEvaluationPlan.HasFieldVector.RichColumnVector
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel

/**
 * Cache on JVM using Spark's storage/manipulation methods
 *
 * The conversion cost for getting the data onto the VE is paid up-front
 * here by pre-converting the data using the Arrow format.
 *
 * The alternate approach is [[InVectorEngineCacheSerializer]].
 */
object ArrowBasedCacheSerializer {

  /**
   * Convert plain Spark internal rows to Arrow Col Batches, and then ByteArray vectors.
   * Closing the Arrow items is not necessary because they are closed in the underlying iterator.
   */
  def sparkInternalRowsToArrowSerializedColBatch(
    internalRows: Iterator[InternalRow],
    arrowSchema: Schema
  )(implicit
    bufferAllocator: BufferAllocator,
    arrowEncodingSettings: ArrowEncodingSettings
  ): Iterator[CachedVeBatch] =
    SparkInternalRowsToArrowColumnarBatches
      .apply(rowIterator = internalRows, arrowSchema = arrowSchema)
      .map { columnarBatch =>
        import com.nec.spark.SparkCycloneExecutorPlugin.source

        CachedVeBatch(DualColumnarBatchContainer(vecs = (0 until columnarBatch.numCols()).map {
          colNo =>
            Right(
              ByteBufferColVector
                .fromArrowVector(columnarBatch.column(colNo).getArrowValueVector)
                .toByteArrayColVector()
            )
        }.toList))
      }

}

/** Non-final as we extend from this */
class ArrowBasedCacheSerializer extends CycloneCacheBase {

  override def convertInternalRowToCachedBatch(
    input: RDD[InternalRow],
    schema: Seq[Attribute],
    storageLevel: StorageLevel,
    conf: SQLConf
  ): RDD[CachedBatch] = {
    implicit val arrowEncodingSettings = ArrowEncodingSettings.fromConf(conf)(input.sparkContext)
    input.mapPartitions(f = internalRows => {
      implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
        .newChildAllocator(s"Writer for partial collector (Arrow)", 0, Long.MaxValue)
      TaskContext.get().addTaskCompletionListener[Unit](_ => allocator.close())
      ArrowBasedCacheSerializer
        .sparkInternalRowsToArrowSerializedColBatch(
          internalRows = internalRows,
          arrowSchema = CycloneCacheBase.makaArrowSchema(schema)
        )
    })
  }

  override def convertColumnarBatchToCachedBatch(
    input: RDD[ColumnarBatch],
    schema: Seq[Attribute],
    storageLevel: StorageLevel,
    conf: SQLConf
  ): RDD[CachedBatch] = input.map { columnarBatch =>
    import com.nec.spark.SparkCycloneExecutorPlugin._
    CachedVeBatch.apply(cachedColumnVectors =
      (0 until columnarBatch.numCols())
        .map(i =>
          ByteBufferColVector
            .fromArrowVector(columnarBatch.column(i).getArrowValueVector)
            .toByteArrayColVector()
        )
        .toList
        .map(byteArrayColVector => Right(byteArrayColVector))
    )
  }

}
