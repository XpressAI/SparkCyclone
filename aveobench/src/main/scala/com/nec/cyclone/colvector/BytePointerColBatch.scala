package com.nec.cyclone.colvector

import com.nec.colvector._
import org.bytedeco.javacpp.BytePointer

final case class BytePointerColBatch(columns: Seq[BytePointerColVector]) {
  private[colvector] def compressedData: BytePointer = {
    // Compute the sizes of all buffers of all columns
    val totalsize = columns.flatMap(_.buffers.map(_.limit()))
      .foldLeft(0L)(_ + _)

    // Create a combined buffer
    val combined = new BytePointer(totalsize)

    // Copy all the bytes over into the combined buffer
    var pos = 0L
    for (column <- columns) {
      for (buffer <- column.buffers) {
        for (i <- 0L.until(buffer.limit())) {
          combined.put(pos, buffer.get(i))
          pos += 1
        }
      }
    }

    combined
  }

  def compressed: CompressedBytePointerColBatch = {
    CompressedBytePointerColBatch(
      columns.map(_.toUnitColVector),
      compressedData
    )
  }
}
