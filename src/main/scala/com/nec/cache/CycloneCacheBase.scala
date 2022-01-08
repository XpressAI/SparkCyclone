package com.nec.cache

import com.nec.spark.SparkCycloneExecutorPlugin
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class CycloneCacheBase extends org.apache.spark.sql.columnar.CachedBatchSerializer {

  override def supportsColumnarOutput(schema: StructType): Boolean = true

  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = true

  override def convertCachedBatchToInternalRow(
    input: RDD[CachedBatch],
    cacheAttributes: Seq[Attribute],
    selectedAttributes: Seq[Attribute],
    conf: SQLConf
  ): RDD[InternalRow] =
    input.mapPartitions(
      preservesPartitioning = true,
      f = { cachedBatchesIter: Iterator[CachedBatch] =>
        lazy implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
          .newChildAllocator(s"Writer for cache collector (Arrow)", 0, Long.MaxValue)

        Iterator
          .continually {
            import scala.collection.JavaConverters._
            cachedBatchesIter
              .map(_.asInstanceOf[CachedVeBatch].dualVeBatch.toEither)
              .flatMap {
                case Left(veColBatch) =>
                  veColBatch.toInternalColumnarBatch().rowIterator().asScala
                case Right(byteArrayColBatch) =>
                  byteArrayColBatch.toInternalColumnarBatch().rowIterator().asScala
              }
          }
          .take(1)
          .flatten
      }
    )

  override def buildFilter(
    predicates: Seq[Expression],
    cachedAttributes: Seq[Attribute]
  ): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = (_, ii) => ii

  override def convertCachedBatchToColumnarBatch(
    input: RDD[CachedBatch],
    cacheAttributes: Seq[Attribute],
    selectedAttributes: Seq[Attribute],
    conf: SQLConf
  ): RDD[ColumnarBatch] = input.map { cachedBatch =>
    cachedBatch.asInstanceOf[CachedVeBatch].dualVeBatch.toInternalColumnarBatch()
  }

}
