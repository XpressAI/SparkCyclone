package io.sparkcyclone.data.vector

import io.sparkcyclone.vectorengine.VeProcess

final case class CompressedVeColBatch private[vector] (columns: Seq[UnitColVector],
                                                          struct: Long,
                                                          buffer: Long) {
  def free()(implicit process: VeProcess): Unit = {
    Seq(struct, buffer).foreach(process.free(_))
  }
}
