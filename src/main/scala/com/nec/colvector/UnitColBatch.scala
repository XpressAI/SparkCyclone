package com.nec.colvector

import com.nec.util.CallContext
import com.nec.ve.VeProcessMetrics
import com.nec.vectorengine.VeProcess

final case class UnitColBatch(columns: Seq[UnitColVector]) {
  def withData(arrays: Seq[Array[Byte]])(implicit source: VeColVectorSource,
                                         process: VeProcess,
                                         context: CallContext,
                                         metrics: VeProcessMetrics): VeColBatch = {
    VeColBatch(columns.zip(arrays).map { case (colvec, bytes) => colvec.withData(bytes) }.map(_.apply()).map(_.get))
  }
}
