package com.nec.cache

import com.nec.cache.VeColColumnarVector.CachedColumnVector
import com.nec.colvector.VeColBatch
import org.apache.spark.sql.columnar.CachedBatch

object CachedVeBatch {
  def apply(cachedColumnVectors: List[CachedColumnVector]): CachedVeBatch = CachedVeBatch(
    DualColumnarBatchContainer(cachedColumnVectors)
  )
  def apply(veColBatch: VeColBatch): CachedVeBatch = CachedVeBatch(
    DualColumnarBatchContainer(veColBatch.columns.map(vcv => Left(vcv)).toList)
  )
}
final case class CachedVeBatch(dualVeBatch: DualColumnarBatchContainer) extends CachedBatch {
  override def numRows: Int = dualVeBatch.numRows

  override def sizeInBytes: Long = dualVeBatch.onCpuSize.getOrElse {
    // cannot represent sizeInBytes here, so use a fairly random number
    100L
  }
}
