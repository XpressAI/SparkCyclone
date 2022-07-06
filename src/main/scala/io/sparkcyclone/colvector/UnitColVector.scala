package io.sparkcyclone.colvector

import io.sparkcyclone.spark.agile.core.{VeString, VeType}
import io.sparkcyclone.util.CallContext
import io.sparkcyclone.vectorengine.{VeAsyncResult, VeProcess}
import io.sparkcyclone.ve.VeProcessMetrics
import java.io.{DataInputStream, DataOutputStream, InputStream}
import java.nio.channels.Channels
import org.bytedeco.javacpp.BytePointer

final case class UnitColVector private[colvector] (
  source: VeColVectorSource,
  name: String,
  veType: VeType,
  numItems: Int,
  dataSize: Option[Int]
) extends ColVectorLike {
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
                                    context: CallContext): () => VeAsyncResult[VeColVector] = {
    import io.sparkcyclone.spark.SparkCycloneExecutorPlugin.veMetrics

    val channel = Channels.newChannel(stream)
    val buffers = bufferSizes.map { size =>
      val bp = new BytePointer(size.toLong)
      val buf = bp.asBuffer()
      var bytesRead = 0
      while (bytesRead < size) {
        bytesRead += channel.read(buf)
      }
      bp
    }

    BytePointerColVector(
      source, name, veType, numItems, buffers,
    ).asyncToVeColVector
  }

  def withData(array: Array[Byte])(implicit source: VeColVectorSource,
                                   process: VeProcess,
                                   context: CallContext,
                                   metrics: VeProcessMetrics): () => VeAsyncResult[VeColVector] = {
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
      ).asyncToVeColVector
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
