package io.sparkcyclone.colvector

import io.sparkcyclone.data.vector._
import io.sparkcyclone.vectorengine.VeProcess

final case class CompressedVeColBatch private[colvector] (columns: Seq[UnitColVector],
                                                          struct: Long,
                                                          buffer: Long) {
  def free()(implicit process: VeProcess): Unit = {
    Seq(struct, buffer).foreach(process.free(_))
  }
}
