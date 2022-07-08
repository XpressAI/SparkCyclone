package io.sparkcyclone.data.vector

import io.sparkcyclone.data.vector._
import io.sparkcyclone.spark.agile.core.{VeScalarType, VeString}
import io.sparkcyclone.vectorengine.{VeAsyncResult, VeProcess}
import org.bytedeco.javacpp.BytePointer

final case class CompressedBytePointerColBatch private[vector] (columns: Seq[UnitColVector],
                                                                   buffer: BytePointer) {
  private[vector] def newCompressedStruct(location: Long): BytePointer = {
    // Get container sizes
    val csizes = columns.map(_.veType.containerSize)

    // Create a combined buffer of the sum of container sizes
    val combined = new BytePointer(csizes.foldLeft(0L)(_ + _))

    // Create data offset
    var dataOffset = 0L
    (columns, csizes.scanLeft(0)(_ + _)).zipped.foreach { case (column, structOffset) =>
      // Get the buffer sizes for the column
      val dlens = column.bufferSizes.map(_.toLong)

      column.veType match {
        case _: VeScalarType =>
          // Each address is the start of the combined data buffer (location) +
          //  offset for the data corresponding to the Nth column +
          //  buffer offset within the column
          combined.putLong(structOffset + 0, location + dataOffset)
          combined.putLong(structOffset + 8, location + dataOffset + dlens(0))
          combined.putInt(structOffset + 16, column.numItems.abs)

        case VeString =>
          val Some(actualDataSize) = column.dataSize
          combined.putLong(structOffset + 0,  location + dataOffset)
          combined.putLong(structOffset + 8,  location + dataOffset + dlens(0))
          combined.putLong(structOffset + 16, location + dataOffset + dlens(0) + dlens(1))
          combined.putLong(structOffset + 24, location + dataOffset + dlens(0) + dlens(1) + dlens(2))
          combined.putInt(structOffset + 32,  actualDataSize.abs)
          combined.putInt(structOffset + 36,  column.numItems.abs)
      }

      // Update the data offset
      dataOffset += dlens.foldLeft(0L)(_ + _)
    }

    combined
  }

  def asyncToCompressedVeColBatch(implicit process: VeProcess): () => VeAsyncResult[CompressedVeColBatch] = {
    // Allocate memory on the VE
    val veLocations = Seq(
      // Size of the compressed struct
      columns.map(_.veType.containerSize.toLong).foldLeft(0L)(_ + _),
      // Size of the combined data
      buffer.limit()
    ).map(process.allocate).map(_.address)

    // Build the compressed struct on VH with the correct pointers to VE memory locations
    val struct = newCompressedStruct(veLocations(1))

    // Construct the CompressedVeColBatch from the VE locations
    val batch = CompressedVeColBatch(columns, veLocations(0), veLocations(1))

    () => {
      val handles = (Seq(struct, buffer), veLocations).zipped.map { case (buf, to) =>
        process.putAsync(buf, to)
      }

      VeAsyncResult(handles: _*) { () =>
        struct.close
        batch
      }
    }
  }

  def toCompressedVeColBatch(implicit process: VeProcess): CompressedVeColBatch = {
    asyncToCompressedVeColBatch.apply().get
  }
}
