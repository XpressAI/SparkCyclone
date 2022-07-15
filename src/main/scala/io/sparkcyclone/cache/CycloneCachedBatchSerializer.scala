package io.sparkcyclone.cache

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.columnar.{CachedBatch, CachedBatchSerializer}
import org.apache.spark.sql.types.StructType

trait CycloneCachedBatchSerializer extends CachedBatchSerializer {
  override def supportsColumnarOutput(schema: StructType): Boolean = {
    true
  }

  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = {
    true
  }

  override def buildFilter(predicates: Seq[Expression], cachedAttributes: Seq[Attribute]):
                          (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
    (_, ii) => ii
  }

  def requiresCleanUp: Boolean
}
