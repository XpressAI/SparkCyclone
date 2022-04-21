package com.nec.colvector

import com.nec.ve.{VeProcess, VeProcessMetrics}
import com.nec.ve.VeProcess.OriginalCallingContext

final case class UnitColBatch(columns: Seq[UnitColVector]) {
  def withData(arrays: Seq[Array[Byte]])(implicit source: VeColVectorSource,
                                         process: VeProcess,
                                         context: OriginalCallingContext,
                                         metrics: VeProcessMetrics): VeColBatch = {
    VeColBatch(columns.zip(arrays).map { case (colvec, bytes) => colvec.withData(bytes) })
  }
}
