package com.nec.cache

import com.nec.spark.planning.VeColColumnarVector.{CachedColVector, DualVeBatch}
import com.nec.ve.VeColBatch
import org.apache.spark.sql.columnar.CachedBatch

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
