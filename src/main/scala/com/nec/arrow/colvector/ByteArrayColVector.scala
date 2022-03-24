package com.nec.arrow.colvector

import com.nec.cache.VeColColumnarVector
import com.nec.ve.VeProcess
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import org.apache.spark.sql.vectorized.ColumnVector
import org.bytedeco.javacpp.BytePointer

/**
 * Storage of a col vector as serialized Arrow buffers
 * We use Option[] because the `container` has no location, only the buffers.
 */
final case class ByteArrayColVector(underlying: GenericColVector[Option[Array[Byte]]]) {

  import underlying._

  def toInternalVector(): ColumnVector =
    new VeColColumnarVector(Right(this), veType.toSparkType)

  def transferBuffersToVe()(implicit
    veProcess: VeProcess,
    source: VeColVectorSource,
    originalCallingContext: OriginalCallingContext
  ): GenericColVector[Option[Long]] =
    underlying.copy(
      buffers = buffers.map { maybeBa =>
        maybeBa.map { ba =>
          /** VE can only take direct byte buffers at the moment */
          val bytePointer = new BytePointer(ba.length)
          bytePointer.put(ba, 0, ba.length)
          bytePointer.position(0)
          veProcess.putPointer(bytePointer)
        }
      },
      container = None,
      source = source
    )

  def transferToBytePointers(): BytePointerColVector =
    BytePointerColVector(underlying.map { baM =>
      baM.map { ba =>
        val bytePointer = new BytePointer(ba.length)
        bytePointer.put(ba, 0, ba.length)
        bytePointer.position(0)
        bytePointer
      }
    })

  /**
   * Compress Array[Byte] list into an Array[Byte]
   */
  def serialize(): Array[Byte] = {
    val totalSize = bufferSizes.sum

    val extractedBuffers = underlying.buffers.flatten

    val resultingArray = Array.ofDim[Byte](totalSize)
    val bufferStarts = extractedBuffers.map(_.length).scanLeft(0)(_ + _)
    bufferStarts.zip(extractedBuffers).foreach { case (start, buffer) =>
      System.arraycopy(buffer, 0, resultingArray, start, buffer.length)
    }

    assert(
      resultingArray.length == totalSize,
      "Resulting array should be same size as sum of all buffer sizes"
    )

    resultingArray
  }
}
