package com.nec.arrow.colvector

import com.nec.spark.agile.core.VeType
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.nec.ve.colvector.VeColVector
import com.nec.ve.{VeProcess, VeProcessMetrics}

import java.io.{DataInputStream, DataOutputStream, InputStream}
import com.nec.spark.agile.core.VeScalarType
import com.nec.spark.agile.core.VeString

final case class UnitColVector private[colvector] (
  source: VeColVectorSource,
  name: String,
  veType: VeType,
  numItems: Int,
  dataSizeO: Option[Int],
) {
  def bufferSizes: Seq[Int] = {
    val validitySize = Math.ceil(numItems / 64.0).toInt * 8

    veType match {
      case stype: VeScalarType =>
        Seq(
          numItems * stype.cSize,
          validitySize
        )

      case VeString =>
        dataSizeO.toSeq.map(_ * 4) ++
        Seq(
          numItems * 4,
          numItems * 4,
          validitySize
        )
    }
  }

  def withData(stream: InputStream)(implicit source: VeColVectorSource,
                                    process: VeProcess,
                                    context: OriginalCallingContext): VeColVector = {
    val buffers = bufferSizes.map { size =>
      process.loadFromStream(stream, size)
    }

    val container = VeColVector.buildContainer(
      veType,
      numItems,
      buffers,
      dataSizeO
    )

    VeColVector(
      GenericColVector(
        source,
        numItems,
        name,
        dataSizeO,
        veType,
        container,
        buffers.toList
      )
    )
  }

  def withData(array: Array[Byte])(implicit source: VeColVectorSource,
                                   process: VeProcess,
                                   context: OriginalCallingContext,
                                   metrics: VeProcessMetrics): VeColVector = {
    metrics.measureRunningTime {
      val buffers = bufferSizes.scanLeft(0)(_ + _).zip(bufferSizes).map {
        case (start, size) => array.slice(start, start + size)
      }

      ByteArrayColVector(
        source,
        numItems,
        name,
        veType,
        buffers
      ).toVeColVector
    }(metrics.registerDeserializationTime)
  }

  def toStream(stream: DataOutputStream): Unit = {
    stream.writeInt(source.identifier.length)
    stream.writeBytes(source.identifier)
    stream.writeInt(name.length)
    stream.writeBytes(name)
    stream.writeInt(UnitColVector.VeTypeToTag(veType))
    stream.writeInt(numItems)
    stream.writeInt(dataSizeO.getOrElse(-1))
  }

  def streamedSize: Int = {
    Seq(4, source.identifier.length, 4, 4, name.size, 4, 4).sum
  }
}

object UnitColVector {
  final lazy val VeTypeToTag: Map[VeType, Int] = {
    VeType.All.zipWithIndex.toMap
  }

  final lazy val VeTagToType: Map[Int, VeType] = {
    VeTypeToTag.map(_.swap)
  }

  def fromStream(stream: DataInputStream): UnitColVector = {
    val source = {
      val len = stream.readInt
      val array = Array.fill[Byte](len)(-1)
      stream.readFully(array)
      VeColVectorSource(new String(array))
    }

    val name = {
      val len = stream.readInt
      val array = Array.fill[Byte](len)(-1)
      stream.readFully(array)
      new String(array)
    }

    val veType = VeTagToType(stream.readInt)
    val numItems = stream.readInt
    val dataSizeO = Option(stream.readInt).filter(_ >= 0)

    UnitColVector(source, name, veType, numItems, dataSizeO)
  }
}



















// /**
//  * Used as a pure carrier class, to ensure type-wise that we are not trying to transfer data itself.
//  */
// final case class UnitColVector(underlying: GenericColVector[Unit]) {
//   def deserializeFromStream(stream: InputStream)
//                            (implicit source: VeColVectorSource,
//                             process: VeProcess,
//                             context: OriginalCallingContext): VeColVector = {

//     val buffers = underlying.bufferSizes.map { size =>
//       process.loadFromStream(stream, size)
//     }

//     val container = VeColVector.buildContainer(
//       underlying.veType,
//       underlying.numItems,
//       buffers,
//       underlying.variableSize
//     )

//     VeColVector(
//       GenericColVector(
//         source,
//         underlying.numItems,
//         underlying.name,
//         underlying.variableSize,
//         underlying.veType,
//         container,
//         buffers
//       )
//     )
//   }

//   import underlying._

//   /**
//    * Decompose the Byte Array and allocate into VeProcess. Uses bufferSizes.
//    *
//    * The parent ColVector is a description of the original source vector from another VE that
//    * could be on an entirely separate machine. Here, by deserializing, we allocate one on our specific VE process.
//    */
//   def deserialize(ba: Array[Byte])(implicit source: VeColVectorSource,
//                                    process: VeProcess,
//                                    context: OriginalCallingContext,
//                                    metrics: VeProcessMetrics): VeColVector = {
//     metrics.measureRunningTime {
//       val buffers = bufferSizes.scanLeft(0)(_ + _).zip(bufferSizes).map {
//         case (bufferStart, bufferSize) =>
//           ba.slice(bufferStart, bufferStart + bufferSize)
//       }

//       ByteArrayColVector(
//         source,
//         numItems,
//         name,
//         veType,
//         buffers
//       ).toVeColVector
//     }(metrics.registerDeserializationTime)
//   }

//   def toStreamFast(dataOutputStream: DataOutputStream): Unit = {
//     dataOutputStream.writeInt(underlying.source.identifier.length)
//     dataOutputStream.writeBytes(underlying.source.identifier)
//     dataOutputStream.writeInt(underlying.numItems)
//     dataOutputStream.writeInt(underlying.name.length)
//     dataOutputStream.writeBytes(underlying.name)
//     dataOutputStream.writeInt(underlying.variableSize.getOrElse(-1))
//     dataOutputStream.writeInt(UnitColVector.veTypeToTag(underlying.veType))
//   }

//   def streamedSize: Int =
//     List(4, underlying.source.identifier.length, 4, 4, underlying.name.size, 4, 4).sum

// }

// object UnitColVector {
//   val veTypeToTag: Map[VeType, Int] = VeType.All.zipWithIndex.toMap
//   val veTagToType: Map[Int, VeType] = veTypeToTag.map(_.swap)

//   def fromStreamFast(dataInputStream: DataInputStream): UnitColVector = {
//     val sourceLen = dataInputStream.readInt()
//     val sourceIdBa = Array.fill[Byte](sourceLen)(-1)
//     dataInputStream.readFully(sourceIdBa)
//     val numItems = dataInputStream.readInt()
//     val nameLen = dataInputStream.readInt()
//     val nameBa = Array.fill[Byte](nameLen)(-1)
//     dataInputStream.readFully(nameBa)
//     val varSize = Option(dataInputStream.readInt()).filter(_ >= 0)
//     val veType = veTagToType(dataInputStream.readInt())
//     UnitColVector(underlying =
//       GenericColVector(
//         source = VeColVectorSource(new String(sourceIdBa)),
//         numItems = numItems,
//         name = new String(nameBa),
//         variableSize = varSize,
//         veType = veType,
//         container = (),
//         buffers = List.fill(GenericColVector.bufCount(veType))(())
//       )
//     )
//   }
// }
