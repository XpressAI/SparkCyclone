package com.nec.arrow.colvector

import com.nec.spark.agile.core.VeType
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.nec.ve.colvector.VeColVector
import com.nec.ve.{VeProcess, VeProcessMetrics}

import java.io.{DataInputStream, DataOutputStream, InputStream}

/**
 * Used as a pure carrier class, to ensure type-wise that we are not trying to transfer data itself.
 */
final case class UnitColVector(underlying: GenericColVector[Unit]) {
  def deserializeFromStream(inStream: InputStream)(implicit
    veProcess: VeProcess,
    originalCallingContext: OriginalCallingContext,
    veColVectorSource: VeColVectorSource
  ): VeColVector = {
    VeColVector(underlying =
      underlying.copy(
        container = -1,
        buffers = underlying.bufferSizes.map { bufSize =>
          veProcess.loadFromStream(inStream, bufSize)
        }
      )
    ).newContainer()
  }

  import underlying._

  /**
   * Decompose the Byte Array and allocate into VeProcess. Uses bufferSizes.
   *
   * The parent ColVector is a description of the original source vector from another VE that
   * could be on an entirely separate machine. Here, by deserializing, we allocate one on our specific VE process.
   */
  def deserialize(ba: Array[Byte])(implicit
    source: VeColVectorSource,
    veProcess: VeProcess,
    originalCallingContext: OriginalCallingContext,
    cycloneMetrics: VeProcessMetrics
  ): VeColVector =
    cycloneMetrics.measureRunningTime {
      VeColVector(
        ByteArrayColVector(
          underlying.copy(
            container = None,
            buffers = bufferSizes.scanLeft(0)(_ + _).zip(bufferSizes).map {
              case (bufferStart, bufferSize) =>
                Option(ba.slice(bufferStart, bufferStart + bufferSize))
            }
          )
        ).transferBuffersToVe()
          .map(_.getOrElse(-1))
      )
        .newContainer()
    }(cycloneMetrics.registerDeserializationTime)

  def toStreamFast(dataOutputStream: DataOutputStream): Unit = {
    dataOutputStream.writeInt(underlying.source.identifier.length)
    dataOutputStream.writeBytes(underlying.source.identifier)
    dataOutputStream.writeInt(underlying.numItems)
    dataOutputStream.writeInt(underlying.name.length)
    dataOutputStream.writeBytes(underlying.name)
    dataOutputStream.writeInt(underlying.variableSize.getOrElse(-1))
    dataOutputStream.writeInt(UnitColVector.veTypeToTag(underlying.veType))
  }

  def streamedSize: Int =
    List(4, underlying.source.identifier.length, 4, 4, underlying.name.size, 4, 4).sum

}

object UnitColVector {
  val veTypeToTag: Map[VeType, Int] = VeType.All.zipWithIndex.toMap
  val veTagToType: Map[Int, VeType] = veTypeToTag.map(_.swap)

  def fromStreamFast(dataInputStream: DataInputStream): UnitColVector = {
    val sourceLen = dataInputStream.readInt()
    val sourceIdBa = Array.fill[Byte](sourceLen)(-1)
    dataInputStream.readFully(sourceIdBa)
    val numItems = dataInputStream.readInt()
    val nameLen = dataInputStream.readInt()
    val nameBa = Array.fill[Byte](nameLen)(-1)
    dataInputStream.readFully(nameBa)
    val varSize = Option(dataInputStream.readInt()).filter(_ >= 0)
    val veType = veTagToType(dataInputStream.readInt())
    UnitColVector(underlying =
      GenericColVector(
        source = VeColVectorSource(new String(sourceIdBa)),
        numItems = numItems,
        name = new String(nameBa),
        variableSize = varSize,
        veType = veType,
        container = (),
        buffers = List.fill(GenericColVector.bufCount(veType))(())
      )
    )
  }
}
