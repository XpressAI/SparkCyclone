package io.sparkcyclone.colvector

import io.sparkcyclone.colvector._
import org.bytedeco.javacpp.BytePointer

object BytePointerColVectorOps {
  implicit class BPCVToCompressedBPCV(vector: BytePointerColVector) {
    def compressed: CompressedBytePointerColVector = {
      // Create a combined buffer
      val combined = new BytePointer(vector.buffers.map(_.limit()).foldLeft(0L)(_ + _))

      // Copy all the bytes over into the combined buffer
      var pos = 0L
      for (buffer <- vector.buffers) {
        for (i <- 0L.until(buffer.limit())) {
          combined.put(pos, buffer.get(i))
          pos += 1
        }
      }

      CompressedBytePointerColVector(
        vector.source,
        vector.name,
        vector.veType,
        vector.numItems,
        combined,
        vector.dataSize
      )
    }
  }
}
