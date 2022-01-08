package com.nec.cache

import com.nec.arrow.colvector.ByteBufferColVector
import com.nec.spark.planning.CEvaluationPlan.HasFieldVector.RichColumnVector
import com.nec.spark.planning.VeColBatchConverters
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
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
class ArrowBasedCacheSerializer extends CycloneCacheBase {

  override def convertInternalRowToCachedBatch(
    input: RDD[InternalRow],
    schema: Seq[Attribute],
    storageLevel: StorageLevel,
    conf: SQLConf
  ): RDD[CachedBatch] =
    VeColBatchConverters
      .internalRowToArrowSerializedColBatch(
        input,
        conf.sessionLocalTimeZone,
        StructType(
          schema.map(att =>
            StructField(
              name = att.name,
              dataType = att.dataType,
              nullable = att.nullable,
              metadata = att.metadata
            )
          )
        ),
        VeColBatchConverters.getNumRows(input.sparkContext, conf)
      )
      .map { cachedColBatchWrapper =>
        CachedVeBatch(cachedColBatchWrapper)
      }

  override def convertColumnarBatchToCachedBatch(
    input: RDD[ColumnarBatch],
    schema: Seq[Attribute],
    storageLevel: StorageLevel,
    conf: SQLConf
  ): RDD[CachedBatch] = input.map { cb =>
    import com.nec.spark.SparkCycloneExecutorPlugin._
    CachedVeBatch.apply(ccv =
      (0 until cb.numCols())
        .map(i =>
          ByteBufferColVector
            .fromArrowVector(cb.column(i).getArrowValueVector)
            .toByteArrayColVector()
        )
        .toList
        .map(b => Right(b))
    )
  }

}
