package com.nec.arrow.colvector

import com.nec.ve.VeProcess
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.nec.ve.colvector.VeColVector
import java.nio.ByteBuffer

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
  def deserialize(
    input: Array[Byte]
  )(implicit source: VeColVectorSource, veProcess: VeProcess): VeColVector = {
    val bufsizes = bufferSizes
    val maxBufSize = if (bufsizes.size > 0) bufsizes.max else -1

    // Allocate ByteBuffer just once
    val tmpbuffer = ByteBuffer.allocateDirect(maxBufSize)

    // Create the VE buffers
    val locations = (bufsizes.scanLeft(0)(_ + _), bufsizes).zipped
      .map { case (offset, size) =>
        // Reset the ByteBuffer's current position
        tmpbuffer.position(0)
        // Copy from Array[Byte] at offset to ByteBuffer, starting from ByteBuffer's current position
        tmpbuffer.put(input, offset, size)
        // Copy from ByteBuffer to VE and get back the VE location
        veProcess.put(tmpbuffer, size)
      }

    VeColVector(
      underlying.copy[Long](
        source = source,
        buffers = locations,
        // Populate later with newContainer()
        container = -1L
      )
    ).newContainer()
  }
}
