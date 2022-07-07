package io.sparkcyclone.data.serialization

import io.sparkcyclone.colvector.{VeColBatch, VeColVectorSource}
import io.sparkcyclone.util.CallContextOps._
import io.sparkcyclone.metrics.VeProcessMetrics
import io.sparkcyclone.vectorengine.{VectorEngine, VeProcess}
import scala.reflect.ClassTag
import java.io.{DataInputStream, EOFException, InputStream}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.serializer.DeserializationStream

object VeDeserializationStream {
  final val DeserStreamed = false
}

class VeDeserializationStream(in: InputStream)(implicit engine: VectorEngine,
                                               metrics: VeProcessMetrics)
                                               extends DeserializationStream with LazyLogging {
  logger.debug(s"Inputting from ==> ${in}; ${in.getClass}")
  val stream = new DataInputStream(in)

  override def readObject[T: ClassTag](): T = {
    in.synchronized {
      stream.readInt match {
        case IntTag             => stream.readInt
        case LongTag            => stream.readLong
        case DoubleTag          => stream.readDouble
        case CbTag              => VeColBatch.fromStream(stream)

        case MixedCbTagColBatch =>
          val size = stream.readInt

          if (VeDeserializationStream.DeserStreamed) {
            metrics.measureRunningTime {
              DualBatchOrBytes.ColBatchWrapper(VeColBatch.fromStream(stream))
            } (metrics.registerDeserializationTime)

          } else {
            metrics.measureRunningTime {
              val bytes = Array.fill[Byte](size)(-1)
              stream.readFully(bytes)
              new DualBatchOrBytes.BytesOnly(bytes, size): DualBatchOrBytes
            } (metrics.registerDeserializationTime)
          }

        case -1 =>
          throw new EOFException

        case other =>
          sys.error(s"Unhandled tag: ${other}")
      }
    }.asInstanceOf[T]
  }

  override def close: Unit = {
    stream.close
  }
}
