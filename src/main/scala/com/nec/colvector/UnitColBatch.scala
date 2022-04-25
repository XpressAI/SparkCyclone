package com.nec.colvector

import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.{VeProcess, VeProcessMetrics}

final case class UnitColBatch(columns: Seq[UnitColVector]) {
  def withData(arrays: Seq[Array[Byte]])(implicit source: VeColVectorSource,
                                         process: VeProcess,
                                         context: OriginalCallingContext,
                                         metrics: VeProcessMetrics): VeColBatch = {
    VeColBatch(columns.zip(arrays).map { case (colvec, bytes) => colvec.withData(bytes) }.map(_.apply()).map(_.get()))
  }
}
