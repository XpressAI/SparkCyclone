package com.nec.cyclone.colvector

import com.nec.colvector._
import com.nec.spark.agile.core.{VeScalarType, VeString, VeType}
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.{VeAsyncResult, VeProcess, VeProcessMetrics}
import org.bytedeco.javacpp.BytePointer

final case class CompressedBytePointerColVector private[colvector] (
  source: VeColVectorSource,
  name: String,
  val veType: VeType,
  val numItems: Int,
  buffer: BytePointer,
  val dataSize: Option[Int]
) extends ColVectorUtilsTrait {

  def asyncToVeColVector(implicit source: VeColVectorSource,
                         process: VeProcess,
                         context: OriginalCallingContext,
                         metrics: VeProcessMetrics): () => VeAsyncResult[VeColVector] = {
    val veLocations = Seq(veType.containerSize.toLong, buffer.limit()).map(process.allocate)
    val lens = bufferSizes

    val structPtr = veType match {
      case stype: VeScalarType =>
        // Declare the struct in host memory
        // The layout of `nullable_T_vector` is the same for all T = primitive
        val ptr = new BytePointer(stype.containerSize.toLong)

        // Assign the data, validity, and count values
        ptr.putLong(0, veLocations(1))
        ptr.putLong(8, veLocations(1) + lens(0))
        ptr.putInt(16, numItems.abs)
        ptr

      case VeString =>
        require(dataSize.nonEmpty, s"dataSize is required to construct container for ${VeString}")
        val Some(actualDataSize) = dataSize
        // Declare the struct in host memory
        val ptr = new BytePointer(VeString.containerSize.toLong)

        // Assign the data, validity, and count values
        ptr.putLong(0,  veLocations(1))
        ptr.putLong(8,  veLocations(1) + lens(0))
        ptr.putLong(16, veLocations(1) + lens(0) + lens(1))
        ptr.putLong(24, veLocations(1) + lens(0) + lens(1) + lens(2))
        ptr.putInt(32,  actualDataSize.abs)
        ptr.putInt(36,  numItems.abs)
        ptr
    }

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
      val handles = (Seq(structPtr, buffer), veLocations).zipped.map { case (buf, to) =>
        process.putAsync(buf, to)
      }

      VeAsyncResult(handles) { () =>
        structPtr.close()
        vector
      }
    }
  }

  def toVeColVector(implicit source: VeColVectorSource,
                    process: VeProcess,
                    context: OriginalCallingContext,
                    metrics: VeProcessMetrics): VeColVector = {
    asyncToVeColVector.apply().get()
  }
}
