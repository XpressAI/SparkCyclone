package com.nec.cyclone.colvector

import com.nec.colvector._
import com.nec.util.CallContext
import com.nec.vectorengine.VeProcess

final case class CompressedVeColBatch private[colvector] (columns: Seq[UnitColVector],
                                                          struct: Long,
                                                          buffer: Long) {
  def free()(implicit process: VeProcess): Unit = {
    Seq(struct, buffer).foreach(process.free(_))
  }
}
