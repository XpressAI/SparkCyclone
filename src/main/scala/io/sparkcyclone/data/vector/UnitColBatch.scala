package io.sparkcyclone.data.vector

import io.sparkcyclone.data.VeColVectorSource
import io.sparkcyclone.metrics.VeProcessMetrics
import io.sparkcyclone.util.CallContext
import io.sparkcyclone.vectorengine.VeProcess

final case class UnitColBatch private[data] (columns: Seq[UnitColVector]) {
  def withData(arrays: Seq[Array[Byte]])(implicit source: VeColVectorSource,
                                         process: VeProcess,
                                         context: CallContext,
                                         metrics: VeProcessMetrics): VeColBatch = {
    VeColBatch(columns.zip(arrays).map { case (colvec, bytes) => colvec.withData(bytes) }.map(_.apply()).map(_.get))
  }
}
