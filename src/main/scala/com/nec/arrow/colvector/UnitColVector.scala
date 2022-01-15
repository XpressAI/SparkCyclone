package com.nec.arrow.colvector

import com.nec.ve.VeProcess
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.nec.ve.colvector.VeColVector

/**
 * Used as a pure carrier class, to ensure type-wise that we are not trying to transfer data itself.
 */
final case class UnitColVector(underlying: GenericColVector[Unit]) {

  import underlying._

  /**
   * Decompose the Byte Array and allocate into VeProcess. Uses bufferSizes.
   *
   * The parent ColVector is a description of the original source vector from another VE that
   * could be on an entirely separate machine. Here, by deserializing, we allocate one on our specific VE process.
   */
  def deserialize(ba: Array[Byte])(implicit
    source: VeColVectorSource,
    veProcess: VeProcess,
    originalCallingContext: OriginalCallingContext
  ): VeColVector =
    VeColVector(
      ByteArrayColVector(
        underlying.copy(
          container = None,
          buffers = bufferSizes.scanLeft(0)(_ + _).zip(bufferSizes).map {
            case (bufferStart, bufferSize) =>
              Option(ba.slice(bufferStart, bufferStart + bufferSize))
          }
        )
      ).transferBuffersToVe()
        .map(_.getOrElse(-1))
    )
      .newContainer()

}
