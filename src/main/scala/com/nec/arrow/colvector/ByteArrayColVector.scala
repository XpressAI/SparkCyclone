package com.nec.arrow.colvector

import com.nec.ve.VeProcess
import com.nec.ve.colvector.VeColBatch.VeColVectorSource

import java.nio.ByteBuffer

/**
 * Storage of a col vector as serialized Arrow buffers
 * We use Option[] because the `container` has no location, only the buffers.
 */
final case class ByteArrayColVector(underlying: GenericColVector[Option[Array[Byte]]]) {
  import underlying._
  def transferBuffersToVe()(implicit
    veProcess: VeProcess,
    source: VeColVectorSource
  ): GenericColVector[Option[Long]] =
    underlying.copy(
      buffers = buffers.map { maybeBa =>
        maybeBa.map { ba =>
          /** VE can only take direct byte buffers at the moment */
          val byteBuffer = ByteBuffer.allocateDirect(ba.length)
          byteBuffer.put(ba, 0, ba.length)
          byteBuffer.position(0)
          veProcess.putBuffer(byteBuffer)
        }
      },
      container = None,
      source = source
    )

  def transferToByteBuffers(): ByteBufferColVector =
    ByteBufferColVector(underlying.map { baM =>
      baM.map { ba =>
        val byteBuffer = ByteBuffer.allocateDirect(ba.length)
        byteBuffer.put(ba, 0, ba.length)
        byteBuffer.position(0)
        byteBuffer
      }
    })

  /**
   * Compress Array[Byte] list into an Array[Byte]
   */
  def serialize(): Array[Byte] = {
    val output = Array.ofDim[Byte](bufferSizes.sum)
    val buffers = underlying.buffers.flatten

    (buffers, buffers.map(_.length).scanLeft(0)(_ + _)).zipped
      .foreach { case (buffer, offset) =>
        System.arraycopy(buffer, 0, output, offset, buffer.length)
      }

    output
  }
}
