package com.nec.arrow.colvector

import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.nec.ve.colvector.VeColVector
import com.nec.ve.{VeProcess, VeProcessMetrics}
import org.bytedeco.javacpp.BytePointer

/**
 * Storage of a col vector as serialized Arrow buffers, that are in BytePointers.
 * We use Option[] because the `container` has no BytePointer.
 */

final case class BytePointerColVector(underlying: GenericColVector[Option[BytePointer]]) {
  def toVeColVector()(implicit
    veProcess: VeProcess,
    _source: VeColVectorSource,
    originalCallingContext: OriginalCallingContext,
    cycloneMetrics: VeProcessMetrics
  ): VeColVector =
    VeColVector(
      transferBuffersToVe()
        .map(_.getOrElse(-1))
    ).newContainer()

  def transferBuffersToVe()(implicit
    veProcess: VeProcess,
    source: VeColVectorSource,
    originalCallingContext: OriginalCallingContext,
    cycloneMetrics: VeProcessMetrics
  ): GenericColVector[Option[Long]] = {
    cycloneMetrics.measureRunningTime(
      underlying
        .map(_.map(bp => {
          veProcess.putPointer(bp)
        }))
        .copy(source = source)
    )(cycloneMetrics.registerTransferTime)
  }

  def toByteArrayColVector(): ByteArrayColVector = {
    ByteArrayColVector(
      underlying.copy(
        container = None,
        buffers = underlying
          .map(_.map(bp => {
            try bp.asBuffer.array()
            catch {
              case _: UnsupportedOperationException =>
                val size = bp.limit()
                val target: Array[Byte] = Array.fill(size.toInt)(-1)
                bp.get(target)
                target
            }
          }))
          .buffers
      )
    )
  }
}
