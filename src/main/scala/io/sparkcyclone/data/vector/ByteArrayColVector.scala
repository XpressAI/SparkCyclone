package io.sparkcyclone.data.vector

import io.sparkcyclone.data.VeColVectorSource
import io.sparkcyclone.native.code.{VeString, VeType}
import io.sparkcyclone.vectorengine.{VeAsyncResult, VeProcess}
import io.sparkcyclone.metrics.VeProcessMetrics
import io.sparkcyclone.util.CallContext
import org.bytedeco.javacpp.BytePointer

final case class ByteArrayColVector private[data] (
  source: VeColVectorSource,
  name: String,
  veType: VeType,
  numItems: Int,
  buffers: Seq[Array[Byte]]
) extends ColVectorLike {
  require(
    numItems >= 0,
    s"[${getClass.getName}] numItems should be >= 0"
  )

  require(
    buffers.size == (if (veType == VeString) 4 else 2),
    s"[${getClass.getName}] Number of Array[Byte]'s does not match the requirement for ${veType}"
  )

  require(
    if (numItems <= 0) {
      // If there are no elements, then all buffers should be zero
      (buffers.filter(_.size <= 0).size == buffers.size)
    } else {
      // Else there should be no empty buffer
      buffers.filter(_.size <= 0).isEmpty
    },
    s"[${getClass.getName}] Should not contain empty Array[Byte]'s"
  )

  def === (other: ByteArrayColVector): Boolean = {
    source == other.source &&
      name == other.name &&
      veType == other.veType &&
      numItems == other.numItems &&
      buffers.map(_.toSeq) == other.buffers.map(_.toSeq)
  }

  def dataSize: Option[Int] = {
    if (veType == VeString) Some(buffers(0).size) else None
  }

  def toBytePointerColVector: BytePointerColVector = {
    val pointers = buffers.map { buffer =>
      // Copy the Array[Byte] to off-heap BytePointer
      new BytePointer(buffer.length).put(buffer, 0, buffer.length).position(0L)
    }

    BytePointerColVector(
      source,
      name,
      veType,
      numItems,
      pointers
    )
  }

  def toVeColVector(implicit veProcess: VeProcess,
                    source: VeColVectorSource,
                    context: CallContext,
                    metrics: VeProcessMetrics): VeColVector = {
    toBytePointerColVector.toVeColVector
  }

  def asyncToVeColVector(implicit veProcess: VeProcess,
                    source: VeColVectorSource,
                    context: CallContext,
                    metrics: VeProcessMetrics): () => VeAsyncResult[VeColVector] = {
    toBytePointerColVector.asyncToVeColVector
  }

  def serialize: Array[Byte] = {
    val lens = buffers.map(_.size)
    val offsets = lens.scanLeft(0)(_ + _)
    val output = Array.ofDim[Byte](lens.foldLeft(0)(_ + _))

    buffers.zip(offsets).foreach { case (buffer, offset) =>
      System.arraycopy(buffer, 0, output, offset, buffer.length)
    }
    output
  }
}
