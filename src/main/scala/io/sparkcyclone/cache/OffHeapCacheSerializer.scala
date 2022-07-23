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
  SQL cache serializer that uses the off-heap memory as the target.

  NOTE: This is still a work in progress, and is currently known to cause JVM
  crashes for yet unknown reasons.
*/
final class OffHeapCacheSerializer extends CycloneCachedBatchSerializer with LazyLogging {
  import SparkCycloneExecutorPlugin._

  def requiresCleanUp: Boolean = {
    true
  }

  override def convertInternalRowToCachedBatch(input: RDD[InternalRow],
                                               attributes: Seq[Attribute],
                                               storageLevel: StorageLevel,
                                               conf: SQLConf): RDD[CachedBatch] = {
    val encoding = ColumnBatchEncoding.fromConf(conf)(input.sparkContext)
    logger.info(s"Converting RDD[InternalRow] to RDD[BytePointerColBatch <: CachedBatch] (target batch size = ${encoding.targetNumRows}) for caching at storage level ${storageLevel}...")
    logger.info(s"Attributes: ${attributes}")
    input.toBytePointerColBatchRDD(attributes, encoding.targetNumRows).map { batch =>
      logger.info(s"RDD[InternalRow] -> RDD[BytePointerColBatch <: CachedBatch]: Generated a batch (rows = ${batch.numRows}, cols = ${batch.numCols}, size = ${batch.sizeInBytes})")
      batch: CachedBatch
    }
  }

  override def convertColumnarBatchToCachedBatch(input: RDD[ColumnarBatch],
                                                 attributes: Seq[Attribute],
                                                 storageLevel: StorageLevel,
                                                 conf: SQLConf): RDD[CachedBatch] = {
    logger.info(s"Converting RDD[ColumnarBatch] to RDD[BytePointerColBatch <: CachedBatch] for caching at storage level ${storageLevel}...")
    logger.info(s"Attributes: ${attributes}")
    val encoding = ColumnBatchEncoding.fromConf(conf)(input.sparkContext)
    input.map(_.toBytePointerColBatch(encoding.makeArrowSchema(attributes))).map { batch =>
      logger.info(s"RDD[ColumnarBatch] -> RDD[BytePointerColBatch <: CachedBatch]: Generated a batch (rows = ${batch.numRows}, cols = ${batch.numCols}, size = ${batch.sizeInBytes})")
      batch: CachedBatch
    }
  }

  override def convertCachedBatchToInternalRow(input: RDD[CachedBatch],
                                               cacheAttributes: Seq[Attribute],
                                               selectedAttributes: Seq[Attribute],
                                               conf: SQLConf): RDD[InternalRow] = {
    logger.info("Converting RDD[BytePointerColBatch <: CachedBatch] to RDD[InternalRow]...")
    logger.info(s"Cached attributes:    ${cacheAttributes}")
    logger.info(s"Selected attributes:  ${selectedAttributes}")
    val columnIndices = selectedAttributes.map(a => cacheAttributes.map(o => o.exprId).indexOf(a.exprId)).toSeq

    input.flatMap { batch0 =>
      BytePointerColBatch(batch0.asInstanceOf[BytePointerColBatch].select(columnIndices)).internalRowIterator
    }
  }

  override def convertCachedBatchToColumnarBatch(input: RDD[CachedBatch],
                                                 cacheAttributes: Seq[Attribute],
                                                 selectedAttributes: Seq[Attribute],
                                                 conf: SQLConf): RDD[ColumnarBatch] = {
    logger.info(s"Converting RDD[BytePointerColBatch <: CachedBatch] to RDD[ColumnarBatch] (selecting ${selectedAttributes.size} of ${cacheAttributes.size} columns)...")
    logger.info(s"Cached attributes:    ${cacheAttributes}")
    logger.info(s"Selected attributes:  ${selectedAttributes}")
    val columnIndices = selectedAttributes.map(a => cacheAttributes.map(o => o.exprId).indexOf(a.exprId)).toSeq

    input.map { batch0 =>
      val batch = BytePointerColBatch(batch0.asInstanceOf[BytePointerColBatch].select(columnIndices)).toSparkColumnarBatch
      logger.info(s"RDD[BytePointerColBatch <: CachedBatch]: Fetched a batch (rows = ${batch.numRows}, cols = ${batch.numCols})")
      batch
    }
  }
}
