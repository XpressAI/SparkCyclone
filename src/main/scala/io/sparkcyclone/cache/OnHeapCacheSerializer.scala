package io.sparkcyclone.cache

import io.sparkcyclone.data.ColumnBatchEncoding
import io.sparkcyclone.data.conversion.SparkSqlColumnarBatchConversions._
import io.sparkcyclone.data.transfer.BpcvTransferDescriptor
import io.sparkcyclone.data.vector._
import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin
import io.sparkcyclone.rdd.RDDConversions._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel

/*
  SQL cache serializer that uses the JVM on-heap memory as the target.

  NOTE: This is still a work in progress, and is currently known to be broken
  for unknown reasons.
*/
final class OnHeapCacheSerializer extends CycloneCachedBatchSerializer with LazyLogging {
  import SparkCycloneExecutorPlugin._

  def requiresCleanUp: Boolean = {
    true
  }

  override def convertInternalRowToCachedBatch(input: RDD[InternalRow],
                                               attributes: Seq[Attribute],
                                               storageLevel: StorageLevel,
                                               conf: SQLConf): RDD[CachedBatch] = {
    val encoding = ColumnBatchEncoding.fromConf(conf)(input.sparkContext)
    logger.info(s"Converting RDD[InternalRow] to RDD[ByteArrayColBatch <: CachedBatch] (batch size ${encoding.targetNumRows}) for caching at storage level ${storageLevel}...")
    input.toBytePointerColBatchRDD(attributes, encoding.targetNumRows).map { batch0 =>
      val batch = batch0.toByteArrayColBatch
      logger.info(s"RDD[InternalRow] -> RDD[ByteArrayColBatch <: CachedBatch]: Generated a batch (rows = ${batch.numRows}, size = ${batch.sizeInBytes})")
      batch: CachedBatch
    }
  }

  override def convertColumnarBatchToCachedBatch(input: RDD[ColumnarBatch],
                                                 attributes: Seq[Attribute],
                                                 storageLevel: StorageLevel,
                                                 conf: SQLConf): RDD[CachedBatch] = {
    logger.info(s"Converting RDD[ColumnarBatch] to RDD[ByteArrayColBatch <: CachedBatch] for caching at storage level ${storageLevel}...")
    val encoding = ColumnBatchEncoding.fromConf(conf)(input.sparkContext)
    input.map { batch0 =>
      val batch = batch0.toBytePointerColBatch(encoding.makeArrowSchema(attributes)).toByteArrayColBatch
      logger.info(s"RDD[ColumnarBatch] -> RDD[ByteArrayColBatch <: CachedBatch]: Generated a batch (rows = ${batch.numRows}, size = ${batch.sizeInBytes})")
      batch: CachedBatch
    }
  }

  override def convertCachedBatchToInternalRow(input: RDD[CachedBatch],
                                               cacheAttributes: Seq[Attribute],
                                               selectedAttributes: Seq[Attribute],
                                               conf: SQLConf): RDD[InternalRow] = {
    logger.info("Converting RDD[ByteArrayColBatch <: CachedBatch] to RDD[InternalRow]...")
    input.flatMap(_.asInstanceOf[ByteArrayColBatch].toBytePointerColBatch.internalRowIterator)
  }

  override def convertCachedBatchToColumnarBatch(input: RDD[CachedBatch],
                                                 cacheAttributes: Seq[Attribute],
                                                 selectedAttributes: Seq[Attribute],
                                                 conf: SQLConf): RDD[ColumnarBatch] = {
    logger.info("Converting RDD[ByteArrayColBatch <: CachedBatch] to RDD[ColumnarBatch]...")
    input.map { batch =>
      logger.info(s"RDD[ByteArrayColBatch <: CachedBatch]: Fetched a batch (rows = ${batch.numRows})")
      batch.asInstanceOf[ByteArrayColBatch].toSparkColumnarBatch
    }
  }
}
