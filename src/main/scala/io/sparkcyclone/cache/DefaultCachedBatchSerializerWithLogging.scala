package io.sparkcyclone.cache

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel

/*
  Default SQL cache serializer that comes with Spark, with logging appended.
*/
class DefaultCachedBatchSerializerWithLogging extends DefaultCachedBatchSerializer with LazyLogging {
  override def convertInternalRowToCachedBatch(input: RDD[InternalRow],
                                               attributes: Seq[Attribute],
                                               storageLevel: StorageLevel,
                                               conf: SQLConf): RDD[CachedBatch] = {
    logger.info(s"Converting RDD[InternalRow] to RDD[CachedBatch] for caching at storage level ${storageLevel}...")
    super.convertInternalRowToCachedBatch(input, attributes, storageLevel, conf).map { batch =>
      logger.info(s"RDD[InternalRow] -> RDD[CachedBatch]: Generated a batch (rows = ${batch.numRows}, size = ${batch.sizeInBytes})")
      batch
    }
  }

  override def convertColumnarBatchToCachedBatch(input: RDD[ColumnarBatch],
                                                 attributes: Seq[Attribute],
                                                 storageLevel: StorageLevel,
                                                 conf: SQLConf): RDD[CachedBatch] = {
    logger.info(s"Converting RDD[ColumnarBatch] to RDD[CachedBatch] for caching at storage level ${storageLevel}...")
    super.convertColumnarBatchToCachedBatch(input, attributes, storageLevel, conf).map { batch =>
      logger.info(s"RDD[ColumnarBatch] -> RDD[CachedBatch]: Generated a batch (rows = ${batch.numRows}, size = ${batch.sizeInBytes})")
      batch
    }
  }

  override def convertCachedBatchToInternalRow(input: RDD[CachedBatch],
                                               cacheAttributes: Seq[Attribute],
                                               selectedAttributes: Seq[Attribute],
                                               conf: SQLConf): RDD[InternalRow] = {
    logger.info("Converting RDD[CachedBatch] to RDD[InternalRow]...")
    logger.info(s"Cached attributes:    ${cacheAttributes}")
    logger.info(s"Selected attributes:  ${selectedAttributes}")
    super.convertCachedBatchToInternalRow(input, cacheAttributes, selectedAttributes, conf)
  }

  override def convertCachedBatchToColumnarBatch(input: RDD[CachedBatch],
                                                 cacheAttributes: Seq[Attribute],
                                                 selectedAttributes: Seq[Attribute],
                                                 conf: SQLConf): RDD[ColumnarBatch] = {
    logger.info(s"Converting RDD[CachedBatch] to RDD[ColumnarBatch] (selecting ${selectedAttributes.size} of ${cacheAttributes.size} columns)...")
    logger.info(s"Cached attributes:    ${cacheAttributes}")
    logger.info(s"Selected attributes:  ${selectedAttributes}")
    super.convertCachedBatchToColumnarBatch(input, cacheAttributes, selectedAttributes, conf).map { batch =>
      logger.info(s"RDD[CachedBatch] -> RDD[ColumnarBatch]: Fetched a batch (rows = ${batch.numRows}, cols = ${batch.numCols})")
      batch
    }
  }
}
