package com.nec.arrow.colvector

import com.nec.ve.VeProcess
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColVector
import com.nec.spark.SparkCycloneExecutorPlugin.metrics.{
  measureRunningTime,
  registerDeserializationTime
}
import com.nec.ve.colvector.VeColBatch.VeColVectorSource

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  InputStream,
  ObjectInputStream,
  ObjectOutputStream
}

/**
 * Used as a pure carrier class, to ensure type-wise that we are not trying to transfer data itself.
 */
final case class UnitColVector(underlying: GenericColVector[Unit]) {

  def byteForm: Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(this)
    oos.flush()
    baos.flush()
    try baos.toByteArray
    finally {
      oos.close()
      baos.close()
    }
  }

  import underlying._

  /**
   * Decompose the Byte Array and allocate into VeProcess. Uses bufferSizes.
   *
   * The parent ColVector is a description of the original source vector from another VE that
   * could be on an entirely separate machine. Here, by deserializing, we allocate one on our specific VE process.
   */
  def deserialize(ba: Array[Byte])(implicit
    veProcess: VeProcess,
    veColVectorSource: VeColVectorSource,
    originalCallingContext: OriginalCallingContext
  ): VeColVector =
    measureRunningTime {
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
    }(registerDeserializationTime)

  def deserializeFromStream(inStream: InputStream)(implicit
    veProcess: VeProcess,
    originalCallingContext: OriginalCallingContext,
    veColVectorSource: VeColVectorSource
  ): VeColVector = {
    VeColVector(underlying =
      underlying.copy(
        container = -1,
        buffers = bufferSizes.map { bufSize =>
          veProcess.loadFromStream(inStream, bufSize)
        }
      )
    ).newContainer()
  }
}

object UnitColVector {
  def fromBytes(arr: Array[Byte]): UnitColVector =
    try fromStream(new ObjectInputStream(new ByteArrayInputStream(arr)))
    catch {
      case e: Throwable =>
        throw new RuntimeException(
          s"Could not deserialize; stream for reading was of size ${arr.size}; ${e}",
          e
        )
    }

  def fromStream(objectInputStream: ObjectInputStream): UnitColVector = {
    objectInputStream
      .readObject()
      .asInstanceOf[UnitColVector]
  }
}
