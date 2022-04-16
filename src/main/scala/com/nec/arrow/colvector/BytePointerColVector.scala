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
  def toVeColVector(implicit source: VeColVectorSource,
                    process: VeProcess,
                    context: OriginalCallingContext,
                    metrics: VeProcessMetrics): VeColVector = {
    val buffers = metrics.measureRunningTime {
      underlying.buffers.flatten.map(process.putPointer)
    }(metrics.registerTransferTime)

    val container = VeColVector.buildContainer(
      underlying.veType,
      underlying.numItems,
      buffers,
      underlying.variableSize
    )

    VeColVector(
      GenericColVector(
        source,
        underlying.numItems,
        underlying.name,
        underlying.variableSize,
        underlying.veType,
        container,
        buffers
      )
    )
  }

  def toByteArrayColVector: ByteArrayColVector = {
    import underlying._

    val buffers = underlying.buffers.flatten.map { ptr =>
      try {
        ptr.asBuffer.array

      } catch {
        case _: UnsupportedOperationException =>
          val output = Array.fill[Byte](ptr.limit().toInt)(-1)
          ptr.get(output)
          output
      }
    }

    ByteArrayColVector(
      source,
      numItems,
      name,
      veType,
      buffers
    )
  }
}
