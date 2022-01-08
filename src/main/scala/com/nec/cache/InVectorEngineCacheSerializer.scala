package com.nec.cache

import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.spark.planning.VeColBatchConverters
import com.nec.ve.VeColBatch
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel

class InVectorEngineCacheSerializer extends CycloneCacheBase {

  override def convertInternalRowToCachedBatch(
    input: RDD[InternalRow],
    schema: Seq[Attribute],
    storageLevel: StorageLevel,
    conf: SQLConf
  ): RDD[CachedBatch] =
    VeColBatchConverters
      .internalRowToVeColBatch(
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
      .map { ui =>
        CachedVeBatch(ui.colBatch)
      }

  override def convertColumnarBatchToCachedBatch(
    input: RDD[ColumnarBatch],
    schema: Seq[Attribute],
    storageLevel: StorageLevel,
    conf: SQLConf
  ): RDD[CachedBatch] = input.map { cb =>
    import com.nec.spark.SparkCycloneExecutorPlugin._

    val vcb = VeColBatch.fromArrowColumnarBatch(cb)
    SparkCycloneExecutorPlugin.register(vcb)
    CachedVeBatch(vcb)
  }

}
