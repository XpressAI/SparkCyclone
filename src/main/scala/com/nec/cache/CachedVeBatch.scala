package com.nec.cache

import com.nec.cache.VeColColumnarVector.CachedColumnVector
import com.nec.ve.VeColBatch
import org.apache.spark.sql.columnar.CachedBatch

object CachedVeBatch {
  def apply(ccv: List[CachedColumnVector]): CachedVeBatch = CachedVeBatch(DualColumnarBatchContainer(ccv))
  def apply(veColBatch: VeColBatch): CachedVeBatch = CachedVeBatch(
    DualColumnarBatchContainer(veColBatch.cols.map(vcv => Left(vcv)))
  )
}
final case class CachedVeBatch(dualVeBatch: DualColumnarBatchContainer) extends CachedBatch {
  override def numRows: Int = dualVeBatch.numRows

  override def sizeInBytes: Long = dualVeBatch.onCpuSize.getOrElse {
    // cannot represent sizeInBytes here, so use a fairly random number
    100L
  }
}
