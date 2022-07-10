package io.sparkcyclone.data.vector

import io.sparkcyclone.data.VeColVectorSource
import io.sparkcyclone.native.code.{VeScalarType, VeString, VeType}
import io.sparkcyclone.vectorengine.{VeAsyncResult, VeProcess}
import org.bytedeco.javacpp.BytePointer

final case class CompressedBytePointerColVector private[vector] (
  source: VeColVectorSource,
  name: String,
  val veType: VeType,
  val numItems: Int,
  buffer: BytePointer,
  val dataSize: Option[Int]
) extends ColVectorLike {
  private[vector] def newStruct(location: Long): BytePointer = {
    val lens = bufferSizes

    veType match {
      case stype: VeScalarType =>
        // Declare the struct in host memory
        // The layout of `nullable_T_vector` is the same for all T = primitive
        val ptr = new BytePointer(stype.containerSize.toLong)

        // Assign the data, validity, and count values
        ptr.putLong(0, location)
        ptr.putLong(8, location + lens(0))
        ptr.putInt(16, numItems.abs)
        ptr

      case VeString =>
        require(dataSize.nonEmpty, s"dataSize is required to construct container for ${VeString}")
        val Some(actualDataSize) = dataSize
        // Declare the struct in host memory
        val ptr = new BytePointer(VeString.containerSize.toLong)

        // Assign the data, validity, and count values
        ptr.putLong(0,  location)
        ptr.putLong(8,  location + lens(0))
        ptr.putLong(16, location + lens(0) + lens(1))
        ptr.putLong(24, location + lens(0) + lens(1) + lens(2))
        ptr.putInt(32,  actualDataSize.abs)
        ptr.putInt(36,  numItems.abs)
        ptr
    }
  }

  def asyncToVeColVector(implicit process: VeProcess): () => VeAsyncResult[VeColVector] = {
    // Allocate memory on the VE
    val veLocations = Seq(veType.containerSize.toLong, buffer.limit()).map(process.allocate).map(_.address)

    // Build the struct on VH with the correct pointers to VE memory locations
    val struct = newStruct(veLocations(1))

    // Generate the VeColVector
    val vector = VeColVector(
      source,
      name,
      veType,
      numItems,
      veLocations.tail,
      dataSize,
      veLocations.head
    )

    () => {
      val handles = (Seq(struct, buffer), veLocations).zipped.map { case (buf, to) =>
        process.putAsync(buf, to)
      }

      VeAsyncResult(handles: _*) { () =>
        struct.close()
        vector
      }
    }
  }

  def toVeColVector(implicit process: VeProcess): VeColVector = {
    asyncToVeColVector.apply().get
  }
}
