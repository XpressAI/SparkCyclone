package com.nec.cache

import com.nec.arrow.ArrowEncodingSettings
import com.nec.arrow.colvector.BytePointerColVector
import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.spark.planning.CEvaluationPlan.HasFieldVector.RichColumnVector
import com.nec.ve.VeColBatch
import com.nec.ve.VeProcess.OriginalCallingContext
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
    bufferAllocator: BufferAllocator,
    arrowEncodingSettings: ArrowEncodingSettings,
    originalCallingContext: OriginalCallingContext
  ): Iterator[CachedVeBatch] = {
    SparkInternalRowsToArrowColumnarBatches
      .apply(rowIterator = rowIterator, arrowSchema = arrowSchema)
      .map { columnarBatch =>
        import SparkCycloneExecutorPlugin._
        val veColBatch = VeColBatch.fromArrowColumnarBatch(columnarBatch)
        SparkCycloneExecutorPlugin.registerCachedBatch(veColBatch)
        try CachedVeBatch(DualColumnarBatchContainer(vecs = veColBatch.cols.map(cv => Left(cv))))
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
    implicit val arrowEncodingSettings = ArrowEncodingSettings.fromConf(conf)(input.sparkContext)
    input.mapPartitions { internalRows =>
      implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
        .newChildAllocator(s"Writer for partial collector (Arrow)", 0, Long.MaxValue)
      TaskContext.get().addTaskCompletionListener[Unit](_ => allocator.close())
      import OriginalCallingContext.Automatic._

      InVectorEngineCacheSerializer
        .internalRowToCachedVeColBatch(
          rowIterator = internalRows,
          arrowSchema = CycloneCacheBase.makaArrowSchema(schema)
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

    import com.nec.spark.SparkCycloneExecutorPlugin._
    columnarBatches.map { columnarBatch =>
      CachedVeBatch.apply(cachedColumnVectors =
        (0 until columnarBatch.numCols())
          .map(i =>
            columnarBatch.column(i).getOptionalArrowValueVector match {
              case Some(acv) =>
                import OriginalCallingContext.Automatic._
                BytePointerColVector
                  .fromArrowVector(acv)
                  .toVeColVector()
              case None =>
                BytePointerColVector
                  .fromColumnarVectorViaArrow(
                    schema(i).name,
                    columnarBatch.column(i),
                    columnarBatch.numRows()
                  ) match {
                  case None =>
                    throw new NotImplementedError(
                      s"Type ${schema(i).dataType} not supported for columnar batch conversion"
                    )
                  case Some((fieldVector, bytePointerColVector)) =>
                    import OriginalCallingContext.Automatic._
                    try bytePointerColVector.toVeColVector()
                    finally fieldVector.close()
                }
            }
          )
          .toList
          .map(byteArrayColVector => Left(byteArrayColVector))
      )
    }
  }

  override def requiresCleanUp: Boolean = false

}
