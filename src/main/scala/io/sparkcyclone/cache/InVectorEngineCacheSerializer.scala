package io.sparkcyclone.cache

import io.sparkcyclone.data.ColumnBatchEncoding
import io.sparkcyclone.data.transfer.{BpcvTransferDescriptor, RowCollectingTransferDescriptor}
import io.sparkcyclone.data.vector.VeColBatch
import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin
import io.sparkcyclone.rdd.RDDConversions._
import io.sparkcyclone.util.CallContext
import scala.util.Try
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel

/*
  SQL cache serializer that uses the Vector Engine as the target.  This does not
  account for VE memory usage at all as Spark API assumes only CPU as a
  Serializer.

  NOTE: This is still a work in progress, and is currently known to be broken
  for unknown reasons.
*/
final class InVectorEngineCacheSerializer extends CycloneCachedBatchSerializer with LazyLogging {
  import SparkCycloneExecutorPlugin._

  def requiresCleanUp: Boolean = {
    false
  }

  override def convertInternalRowToCachedBatch(input: RDD[InternalRow],
                                               attributes: Seq[Attribute],
                                               storageLevel: StorageLevel,
                                               conf: SQLConf): RDD[CachedBatch] = {
    val encoding = ColumnBatchEncoding.fromConf(conf)(input.sparkContext)
    logger.info(s"Converting RDD[InternalRow] to RDD[VeColBatch <: CachedBatch] (batch size ${encoding.targetNumRows}) for caching at storage level ${storageLevel}...")
    input.toVeColBatchRDD(attributes, encoding.targetNumRows).map { batch =>
      logger.info(s"RDD[InternalRow] -> RDD[VeColBatch <: CachedBatch]: Generated a batch (rows = ${batch.numRows}, size = ${batch.sizeInBytes})")
      batch: CachedBatch
    }
  }

  override def convertColumnarBatchToCachedBatch(input: RDD[ColumnarBatch],
                                                 attributes: Seq[Attribute],
                                                 storageLevel: StorageLevel,
                                                 conf: SQLConf): RDD[CachedBatch] = {
    logger.info(s"Converting RDD[ColumnarBatch] to RDD[VeColBatch <: CachedBatch] for caching at storage level ${storageLevel}...")
    val encoding = ColumnBatchEncoding.fromConf(conf)(input.sparkContext)
    input.toVeColBatchRDD(encoding.makeArrowSchema(attributes)).map { batch =>
      logger.info(s"RDD[ColumnarBatch] -> RDD[VeColBatch <: CachedBatch]: Generated a batch (rows = ${batch.numRows}, size = ${batch.sizeInBytes})")
      batch: CachedBatch
    }
  }

  override def convertCachedBatchToInternalRow(input: RDD[CachedBatch],
                                               cacheAttributes: Seq[Attribute],
                                               selectedAttributes: Seq[Attribute],
                                               conf: SQLConf): RDD[InternalRow] = {
    logger.info("Converting RDD[VeColBatch <: CachedBatch] to RDD[InternalRow]...")
    input.flatMap(_.asInstanceOf[VeColBatch].toBytePointerColBatch.internalRowIterator)
  }

  override def convertCachedBatchToColumnarBatch(input: RDD[CachedBatch],
                                                 cacheAttributes: Seq[Attribute],
                                                 selectedAttributes: Seq[Attribute],
                                                 conf: SQLConf): RDD[ColumnarBatch] = {
    logger.info("Converting RDD[VeColBatch <: CachedBatch] to RDD[ColumnarBatch]...")
    input.map { batch =>
      logger.info(s"RDD[VeColBatch <: CachedBatch] -> RDD[ColumnarBatch]: Fetched a batch (rows = ${batch.numRows})")
      batch.asInstanceOf[VeColBatch].toSparkColumnarBatch
    }
  }
}
