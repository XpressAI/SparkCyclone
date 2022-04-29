package com.nec.cyclone.colvector

import com.nec.colvector._
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.VeProcess

final case class CompressedVeColBatch private[colvector] (columns: Seq[UnitColVector],
                                                          struct: Long,
                                                          buffer: Long) {
  def free()(implicit source: VeColVectorSource,
             process: VeProcess,
             context: OriginalCallingContext): Unit = {
    Seq(struct, buffer).foreach(process.free)
  }
}
