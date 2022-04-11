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
  require(buffers.size > 0, s"${getClass.getName} should contain at least one Array[Byte] buffer")
  require(buffers.filter(_.size <= 0).isEmpty, s"${getClass.getName} should not contain empty Array[Byte]'s")

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

// /**
//  * Storage of a col vector as serialized Arrow buffers
//  * We use Option[] because the `container` has no location, only the buffers.
//  */
// final case class ByteArrayColVector(underlying: GenericColVector[Option[Array[Byte]]]) {

//   import underlying._

//   def toInternalVector(): ColumnVector =
//     new VeColColumnarVector(Right(this), veType.toSparkType)

//   def transferBuffersToVe()(implicit
//     veProcess: VeProcess,
//     source: VeColVectorSource,
//     originalCallingContext: OriginalCallingContext
//   ): GenericColVector[Option[Long]] =
//     underlying.copy(
//       buffers = buffers.map { maybeBa =>
//         maybeBa.map { ba =>
//           /** VE can only take direct byte buffers at the moment */
//           val bytePointer = new BytePointer(ba.length)
//           bytePointer.put(ba, 0, ba.length)
//           bytePointer.position(0)
//           veProcess.putPointer(bytePointer)
//         }
//       },
//       container = None,
//       source = source
//     )

//   def transferToBytePointers(): BytePointerColVector =
//     BytePointerColVector(underlying.map { baM =>
//       baM.map { ba =>
//         val bytePointer = new BytePointer(ba.length)
//         bytePointer.put(ba, 0, ba.length)
//         bytePointer.position(0)
//         bytePointer
//       }
//     })

//   /**
//    * Compress Array[Byte] list into an Array[Byte]
//    */
//   def serialize(): Array[Byte] = {
//     val totalSize = bufferSizes.sum

//     val extractedBuffers = underlying.buffers.flatten

//     val resultingArray = Array.ofDim[Byte](totalSize)
//     val bufferStarts = extractedBuffers.map(_.length).scanLeft(0)(_ + _)
//     bufferStarts.zip(extractedBuffers).foreach { case (start, buffer) =>
//       System.arraycopy(buffer, 0, resultingArray, start, buffer.length)
//     }

//     assert(
//       resultingArray.length == totalSize,
//       "Resulting array should be same size as sum of all buffer sizes"
//     )

//     resultingArray
//   }
// }
