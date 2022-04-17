package com.nec.arrow.colvector

import com.nec.spark.agile.core.{VeScalarType, VeString, VeType}
import com.nec.ve.{VeProcess, VeProcessMetrics}
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.nec.ve.colvector.VeColVector
import java.io.{DataInputStream, DataOutputStream, InputStream}

final case class UnitColVector private[colvector] (
  source: VeColVectorSource,
  name: String,
  veType: VeType,
  numItems: Int,
  dataSize: Option[Int],
) extends ColVectorUtilsTrait {
  require(
    numItems >= 0,
    s"[${getClass.getName}] numItems should be >= 0"
  )

  require(
    if (veType == VeString) dataSize.nonEmpty else dataSize.isEmpty,
    s"""VE type is ${veType} but dataSize ${if (dataSize.isEmpty) "is" else "is not"} empty"""
  )

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
      dataSize
    )

    VeColVector(
      GenericColVector(
        source,
        numItems,
        name,
        dataSize,
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
        name,
        veType,
        numItems,
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
    stream.writeInt(dataSize.getOrElse(-1))
  }

  def streamedSize: Int = {
    Seq(4, source.identifier.length, 4, 4, name.size, 4, 4).sum
  }
}

object UnitColVector {
  private[colvector] final lazy val VeTypeToTag: Map[VeType, Int] = {
    VeType.All.zipWithIndex.toMap
  }

  private[colvector] final lazy val VeTagToType: Map[Int, VeType] = {
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
