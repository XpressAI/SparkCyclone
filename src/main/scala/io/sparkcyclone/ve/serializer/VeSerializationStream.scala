package io.sparkcyclone.ve.serializer

import io.sparkcyclone.colvector.VeColBatch
import io.sparkcyclone.ve.serializer.DualBatchOrBytes.{BytesOnly, ColBatchWrapper}
import io.sparkcyclone.ve.VeProcessMetrics
import io.sparkcyclone.vectorengine.VeProcess
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.serializer.SerializationStream
import java.io.{DataOutputStream, OutputStream}
import scala.reflect.ClassTag

class VeSerializationStream(out: OutputStream)(implicit veProcess: VeProcess,
                                               metrics: VeProcessMetrics)
                                               extends SerializationStream with LazyLogging {
  logger.debug(s"Outputting to ==> ${out}; ${out.getClass}")
  val stream = new DataOutputStream(out)

  override def writeObject[T: ClassTag](obj: T): SerializationStream = {
    obj match {
      case i: java.lang.Integer =>
        stream.writeInt(IntTag)
        stream.writeInt(i)

      case i: java.lang.Long =>
        stream.writeInt(LongTag)
        stream.writeLong(i)

      case i: java.lang.Double =>
        stream.writeInt(DoubleTag)
        stream.writeDouble(i)

      case v: VeColBatch =>
        stream.writeInt(CbTag)
        v.toStream(stream)

      case v: BytesOnly =>
        logger.debug(s"Will write ${v.size} (${v.bytes.length} as MixedBatch")
        stream.writeInt(MixedCbTagColBatch)
        stream.writeInt(v.size)
        stream.write(v.bytes)

      case v: ColBatchWrapper =>
        stream.writeInt(MixedCbTagColBatch)

        metrics.measureRunningTime {
          stream.writeInt(v.veColBatch.streamedSize)
          val startSize = stream.size
          v.veColBatch.toStream(stream)
          stream.flush
          val endSize = stream.size
          val diff = endSize - startSize
          require(
            diff == v.veColBatch.streamedSize,
            s"Written ${diff} bytes, expected ${v.veColBatch.streamedSize}"
          )
        } (metrics.registerSerializationTime)

      case other =>
        sys.error(s"Not supported here to write item of type ${other.getClass.getCanonicalName}: ($other)")
    }

    this
  }

  override def flush: Unit = {
    stream.flush
  }

  override def close: Unit = {
    stream.close
  }
}
