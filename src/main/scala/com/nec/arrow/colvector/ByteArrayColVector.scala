package com.nec.arrow.colvector

import com.nec.cache.VeColColumnarVector
import com.nec.spark.agile.core.VeType
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import org.apache.spark.sql.vectorized.ColumnVector
import org.bytedeco.javacpp.BytePointer
import com.nec.spark.agile.core.VeString

final case class ByteArrayColVector private[colvector] (
  source: VeColVectorSource,
  numItems: Int,
  name: String,
  veType: VeType,
  buffers: Seq[Array[Byte]]
) {
  require(
    buffers.size == (if (veType == VeString) 4 else 2),
    s"${getClass.getName} Number of Array[Byte]'s does not match the requirement for ${veType}"
  )

  require(
    if (numItems <= 0) {
      // If there are no elements, then all buffers should be zero
      (buffers.filter(_.size <= 0).size == buffers.size)
    } else {
      // Else there should be no empty buffer
      buffers.filter(_.size <= 0).isEmpty
    },
    s"${getClass.getName} should not contain empty Array[Byte]'s"
  )

  def toSparkColumnVector: ColumnVector = {
    new VeColColumnarVector(Right(this), veType.toSparkType)
  }

  def toBytePointerColVector: BytePointerColVector = {
    val pointers = buffers.map { buffer =>
      // Copy the Array[Byte] to off-heap BytePointer
      val ptr = new BytePointer(buffer.length)
      ptr.put(buffer, 0, buffer.length)
      ptr.position(0)
    }

    BytePointerColVector(
      GenericColVector(
        source,
        numItems,
        name,
        // Populate variableSize for the VeString case
        // The first Array[Byte] is where the data is stored
        if (veType == VeString) Some(pointers.head.limit().toInt / 4) else None,
        veType,
        None,
        pointers.map(Some(_)).toList
      )
    )
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
