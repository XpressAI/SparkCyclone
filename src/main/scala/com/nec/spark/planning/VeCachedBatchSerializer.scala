package com.nec.spark.planning

import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.spark.SparkCycloneExecutorPlugin.source
import com.nec.spark.planning.ArrowBatchToUnsafeRows.mapBatchToRow
import com.nec.spark.planning.VeCachedBatchSerializer.{CachedVeBatch, ShortCircuit}
import com.nec.spark.planning.VeColColumnarVector.{CachedColVector, DualVeBatch}
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
  object CachedVeBatch {
    def apply(ccv: List[CachedColVector]): CachedVeBatch = CachedVeBatch(DualVeBatch(ccv))
    def apply(veColBatch: VeColBatch): CachedVeBatch = CachedVeBatch(
      DualVeBatch(veColBatch.cols.map(vcv => Left(vcv)))
    )
  }
  final case class CachedVeBatch(dualVeBatch: DualVeBatch) extends CachedBatch {
    override def numRows: Int = dualVeBatch.numRows

    override def sizeInBytes: Long = dualVeBatch.onCpuSize.getOrElse {
      // cannot represent sizeInBytes here, so use a fairly random number
      100L
    }
  }

  val ShortCircuit = true

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
  ): RDD[CachedBatch] = input.map(cb => {
    import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
    val vcb = VeColBatch.fromArrowColumnarBatch(cb)
    SparkCycloneExecutorPlugin.register(vcb)
    CachedVeBatch(vcb)
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

    import SparkCycloneExecutorPlugin._
    if (ShortCircuit)
      cachedBatch.asInstanceOf[CachedVeBatch].dualVeBatch.toInternalColumnarBatch()
    else {
      lazy implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
        .newChildAllocator(s"Writer for cache collector (Arrow)", 0, Long.MaxValue)
      cachedBatch.asInstanceOf[CachedVeBatch].dualVeBatch.toArrowColumnarBatch()
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
            import SparkCycloneExecutorPlugin._
            lazy implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
              .newChildAllocator(s"Writer for cache collector (Arrow)", 0, Long.MaxValue)
            mapBatchToRow(
              cachedBatch.asInstanceOf[CachedVeBatch].dualVeBatch.toArrowColumnarBatch()
            )
          }
          .take(1)
          .flatten
      }
    else
      convertCachedBatchToColumnarBatch(input, cacheAttributes, selectedAttributes, conf)
        .mapPartitions(columnarBatchIterator => columnarBatchIterator.flatMap(mapBatchToRow))

}
