package com.nec.spark.planning

import com.nec.spark.planning.VeCachedBatchSerializer.{CachedVeBatch, ShortCircuit}
import com.nec.ve.VeColBatch
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel

object VeCachedBatchSerializer {
  final case class CachedVeBatch(veColBatch: VeColBatch) extends CachedBatch {
    override def numRows: Int = veColBatch.numRows

    override def sizeInBytes: Long = 100
  }

  val ShortCircuit = true

  def unwrapBatch(columnarBatch: ColumnarBatch): VeColBatch =
    VeColBatch(
      numRows = columnarBatch.numRows(),
      cols = (0 until columnarBatch.numCols())
        .map(col => col.asInstanceOf[VeColColumnarVector].veColVector)
        .toList
    )
}

class VeCachedBatchSerializer extends org.apache.spark.sql.columnar.CachedBatchSerializer {
  override def supportsColumnarOutput(schema: StructType): Boolean = true

  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = true

  override def convertInternalRowToCachedBatch(
    input: RDD[InternalRow],
    schema: Seq[Attribute],
    storageLevel: StorageLevel,
    conf: SQLConf
  ): RDD[CachedBatch] = VeColBatchConverters
    .internalRowToVeColBatch(
      input,
      conf.sessionLocalTimeZone,
      StructType(
        schema.map(att => StructField(att.name, att.dataType, att.nullable, att.metadata))
      ),
      VeColBatchConverters.getNumRows(input.sparkContext, conf)
    )
    .map(CachedVeBatch)

  override def convertColumnarBatchToCachedBatch(
    input: RDD[ColumnarBatch],
    schema: Seq[Attribute],
    storageLevel: StorageLevel,
    conf: SQLConf
  ): RDD[CachedBatch] = input.map(cb => {
    import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
    CachedVeBatch(VeColBatch.fromColumnarBatch(cb))
  })

  override def buildFilter(
    predicates: Seq[Expression],
    cachedAttributes: Seq[Attribute]
  ): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = (i, ii) => ii

  override def convertCachedBatchToColumnarBatch(
    input: RDD[CachedBatch],
    cacheAttributes: Seq[Attribute],
    selectedAttributes: Seq[Attribute],
    conf: SQLConf
  ): RDD[ColumnarBatch] = input.map(cachedBatch => {
    import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
    if (ShortCircuit)
      cachedBatch.asInstanceOf[CachedVeBatch].veColBatch.toInternalColumnarBatch()
    else {
      lazy implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
        .newChildAllocator(s"Writer for cache collector (Arrow)", 0, Long.MaxValue)
      cachedBatch.asInstanceOf[CachedVeBatch].veColBatch.toArrowColumnarBatch()
    }
  })

  override def convertCachedBatchToInternalRow(
    input: RDD[CachedBatch],
    cacheAttributes: Seq[Attribute],
    selectedAttributes: Seq[Attribute],
    conf: SQLConf
  ): RDD[InternalRow] =
    if (ShortCircuit)
      input.flatMap { cachedBatch =>
        Iterator
          .continually {
            import com.nec.spark.SparkCycloneExecutorPlugin.veProcess

            lazy implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
              .newChildAllocator(s"Writer for cache collector (Arrow)", 0, Long.MaxValue)
            ArrowColumnarToRowPlan.mapBatchToRow(
              cachedBatch.asInstanceOf[CachedVeBatch].veColBatch.toArrowColumnarBatch()
            )
          }
          .take(1)
          .flatten
      }
    else
      convertCachedBatchToColumnarBatch(input, cacheAttributes, selectedAttributes, conf)
        .mapPartitions(columnarBatchIterator =>
          columnarBatchIterator.flatMap(ArrowColumnarToRowPlan.mapBatchToRow)
        )

}
