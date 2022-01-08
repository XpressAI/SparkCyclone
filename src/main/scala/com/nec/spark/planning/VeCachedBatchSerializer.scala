package com.nec.spark.planning

import com.nec.spark.planning.VeCachedBatchSerializer.ArrowSerializedCachedBatch
import com.nec.ve.ByteArrayColVector._
import com.nec.ve.VeColBatch.{VeColVectorSource, VectorEngineLocation}
import com.nec.ve.{ByteBufferVeColVector, MaybeByteArrayColVector, VeColBatch, VeProcess}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel
object VeCachedBatchSerializer {

  final case class ArrowSerializedCachedBatch(numRows: Int, cols: List[MaybeByteArrayColVector])
    extends CachedBatch {
    override def sizeInBytes: Long = cols.flatMap(_.buffers).map(_.fold(0)(_.length)).sum

    def toVeColumnarBatch(): ColumnarBatch = {
      val vecs = cols.map(cv => cv.toInternalVector)
      val cb = new ColumnarBatch(vecs.toArray, cols.head.numItems)
      cb.setNumRows(cols.head.numItems)
      cb
    }
  }

  def unwrapIntoVE(
    columnarBatch: ColumnarBatch
  )(implicit veProcess: VeProcess, veColVectorSource: VeColVectorSource): VeColBatch =
    VeColBatch(
      numRows = columnarBatch.numRows(),
      cols = (0 until columnarBatch.numCols())
        .map(colIdx =>
          columnarBatch
            .column(colIdx)
            .asInstanceOf[ArrowSerializedColColumnarVector]
            .veColVector
            .transferBuffersToVe()
            .map(_.getOrElse(VectorEngineLocation(-1)))
            .newContainer()
        )
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
    .internalRowToArrowSerializedBatch(
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

  override def convertColumnarBatchToCachedBatch(
    input: RDD[ColumnarBatch],
    schema: Seq[Attribute],
    storageLevel: StorageLevel,
    conf: SQLConf
  ): RDD[CachedBatch] = input.map(cb => {
    import ByteBufferVeColVector._
    import com.nec.spark.SparkCycloneExecutorPlugin.source
    ArrowSerializedCachedBatch(
      numRows = cb.numRows(),
      cols = (0 until cb.numCols())
        .map(c =>
          ByteBufferVeColVector
            .fromVectorColumn(cb.column(c))
            .serializeBuffers()
        )
        .toList
    ): CachedBatch
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
    cachedBatch.asInstanceOf[ArrowSerializedCachedBatch].toVeColumnarBatch()
  })

  import scala.collection.JavaConverters._
  override def convertCachedBatchToInternalRow(
    input: RDD[CachedBatch],
    cacheAttributes: Seq[Attribute],
    selectedAttributes: Seq[Attribute],
    conf: SQLConf
  ): RDD[InternalRow] =
    input.mapPartitions { cachedBatches =>
      cachedBatches.flatMap(
        _.asInstanceOf[ArrowSerializedCachedBatch].toVeColumnarBatch().rowIterator().asScala
      )
    }

}
