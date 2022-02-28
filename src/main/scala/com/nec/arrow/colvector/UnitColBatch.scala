package com.nec.arrow.colvector

import com.nec.ve.{VeProcess, VeProcessMetrics}
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch
import com.nec.ve.colvector.VeColBatch.VeColVectorSource

final case class UnitColBatch(underlying: GenericColBatch[UnitColVector]) {
  def deserialize(seqs: List[Array[Byte]])(implicit
    veProcess: VeProcess,
    originalCallingContext: OriginalCallingContext,
    veColVectorSource: VeColVectorSource,
    cycloneMetrics: VeProcessMetrics
  ): VeColBatch = {
    val theMap = underlying.cols
      .zip(seqs)
      .map { case (unitCv, bytes) =>
        unitCv -> unitCv.deserialize(bytes)
      }
      .toMap

    VeColBatch(underlying.map(ucv => theMap(ucv)))
  }
}
