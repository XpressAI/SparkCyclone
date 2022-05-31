package com.nec.cyclone.colvector

import com.nec.colvector._
import com.nec.util.CallContext
import com.nec.vectorengine.VeProcess

final case class CompressedVeColBatch private[colvector] (columns: Seq[UnitColVector],
                                                          struct: Long,
                                                          buffer: Long) {
  def free()(implicit source: VeColVectorSource,
             process: VeProcess,
             context: CallContext): Unit = {
    Seq(struct, buffer).foreach(process.free)
  }
}
