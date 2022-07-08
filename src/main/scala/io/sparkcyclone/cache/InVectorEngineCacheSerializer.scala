package io.sparkcyclone.cache

import io.sparkcyclone.data.conversion.ArrowVectorConversions._
import io.sparkcyclone.data.conversion.SparkSqlColumnVectorConversions._
import io.sparkcyclone.data.vector.VeColBatch
import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin
import io.sparkcyclone.util.CallContext
import io.sparkcyclone.metrics.VeProcessMetrics
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
 * Cache that is using the Vector Engine as the target.
 * This does not account for VE memory usage at all as Spark API assumes only CPU as a Serializer.
 * The alternate approach is [[ArrowBasedCacheSerializer]].
 */
object InVectorEngineCacheSerializer {

  /**
   * Convert Spark's InternalRow to cached VeColBatch.
   * Dual-mode is not considered here as the input is expected to be plain Spark input.
   *
   * Automatically register it to the Executor's cache registry
   */
  def internalRowToCachedVeColBatch(rowIterator: Iterator[InternalRow], arrowSchema: Schema)(
    implicit
    allocator: BufferAllocator,
    encoding: ArrowEncodingSettings,
    context: CallContext,
    cycloneMetrics: VeProcessMetrics
  ): Iterator[CachedVeBatch] = {
    SparkInternalRowsToArrowColumnarBatches
      .apply(rowIterator = rowIterator, arrowSchema = arrowSchema)
      .map { columnarBatch =>
        import SparkCycloneExecutorPlugin._
        val veColBatch = VeColBatch.fromArrowColumnarBatch(columnarBatch)
        // SparkCycloneExecutorPlugin.batchesCache.register(veColBatch)
        try CachedVeBatch(DualColumnarBatchContainer(vecs = veColBatch.columns.map(cv => Left(cv)).toList))
        finally columnarBatch.close()
      }
  }

}

/** Non-final as we extend from this */
class InVectorEngineCacheSerializer extends CycloneCacheBase {

  override def convertInternalRowToCachedBatch(
    input: RDD[InternalRow],
    schema: Seq[Attribute],
    storageLevel: StorageLevel,
    conf: SQLConf
  ): RDD[CachedBatch] = {
    implicit val encoding = ArrowEncodingSettings.fromConf(conf)(input.sparkContext)
    input.mapPartitions { internalRows =>
      implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
        .newChildAllocator(s"Writer for partial collector (Arrow)", 0, Long.MaxValue)
      TaskContext.get().addTaskCompletionListener[Unit](_ => allocator.close())
      import io.sparkcyclone.util.CallContextOps._
      import SparkCycloneExecutorPlugin._
      InVectorEngineCacheSerializer
        .internalRowToCachedVeColBatch(
          rowIterator = internalRows,
          arrowSchema = CycloneCacheBase.makeArrowSchema(schema)
        )
    }
  }

  override def convertColumnarBatchToCachedBatch(
    input: RDD[ColumnarBatch],
    schema: Seq[Attribute],
    storageLevel: StorageLevel,
    conf: SQLConf
  ): RDD[CachedBatch] = input.mapPartitions { columnarBatches =>
    implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
      .newChildAllocator(s"Writer for partial collector (Arrow)", 0, Long.MaxValue)
    TaskContext.get().addTaskCompletionListener[Unit](_ => allocator.close())

    import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin._
    columnarBatches.map { columnarBatch =>
      CachedVeBatch.apply(cachedColumnVectors =
        (0 until columnarBatch.numCols())
          .map { i =>
            import io.sparkcyclone.util.CallContextOps._
            columnarBatch.column(i).getOptionalArrowValueVector match {
              case Some(acv) =>
                acv.toBytePointerColVector.asyncToVeColVector
              case None =>
                columnarBatch.column(i)
                  .toBytePointerColVector(schema(i).name, columnarBatch.numRows)
                  .asyncToVeColVector
            }
          }
          .map(_.apply())
          .map(_.get)
          .toList
          .map(byteArrayColVector => Left(byteArrayColVector))
      )
    }
  }

  override def requiresCleanUp: Boolean = false

}