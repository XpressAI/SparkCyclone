package com.nec.cache

import com.nec.cache.DualMode.cachedBatchesToDualModeInternalRows
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * This base decodes both variations of [[ArrowBasedCacheSerializer]] and [[InVectorEngineCacheSerializer]].
 *
 * We are using Serializer as opposed to a custom CacheManager due to the complexity of implementing one.
 * Serializer works, mostly.
 */
abstract class CycloneCacheBase extends org.apache.spark.sql.columnar.CachedBatchSerializer {

  override def supportsColumnarOutput(schema: StructType): Boolean = true

  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = true

  /**
   * Create an 'internal columnarBatch', and then use a 'rowIterator' which emits 'ColumnarBatchRow' which,
   * when consumed, will contain the original columnar batch.
   *
   * This is done in order to allow the reading of cache through the InMemoryExec, as that
   * does not always seem to support a ColumnarBatch approach.
   *
   * Effectively, this is a hack-around.
   */
  override def convertCachedBatchToInternalRow(
    input: RDD[CachedBatch],
    cacheAttributes: Seq[Attribute],
    selectedAttributes: Seq[Attribute],
    conf: SQLConf
  ): RDD[InternalRow] =
    input.mapPartitions(preservesPartitioning = true, f = cachedBatchesToDualModeInternalRows)

  override def convertCachedBatchToColumnarBatch(
    input: RDD[CachedBatch],
    cacheAttributes: Seq[Attribute],
    selectedAttributes: Seq[Attribute],
    conf: SQLConf
  ): RDD[ColumnarBatch] = input.map { cachedBatch =>
    cachedBatch.asInstanceOf[CachedVeBatch].dualVeBatch.toInternalColumnarBatch
  }

  override def buildFilter(
    predicates: Seq[Expression],
    cachedAttributes: Seq[Attribute]
  ): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = (_, ii) => ii

  def requiresCleanUp: Boolean

}
object CycloneCacheBase {

  def makaArrowSchema(
    schema: Seq[Attribute]
  )(implicit arrowEncodingSettings: ArrowEncodingSettings): Schema =
    ArrowUtilsExposed.toArrowSchema(
      schema = StructType(
        schema.map(att =>
          StructField(
            name = att.name,
            dataType = att.dataType,
            nullable = att.nullable,
            metadata = att.metadata
          )
        )
      ),
      timeZoneId = arrowEncodingSettings.timezone
    )
}
