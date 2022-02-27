package com.nec.ve.serializer

import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.ve.VeProcess
import com.nec.ve.colvector.VeColBatch
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.nec.ve.serializer.DualBatchOrBytes.{BytesOnly, ColBatchWrapper}
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializationStream

import java.io.{DataOutputStream, OutputStream}
import scala.reflect.ClassTag

class VeSerializationStream(out: OutputStream)(implicit
  veProcess: VeProcess,
  veColVectorSource: VeColVectorSource
) extends SerializationStream
  with Logging {
  val dataOutputStream = new DataOutputStream(out)
  logDebug(s"Outputting to ==> ${out}; ${out.getClass}")

  /**
   * Generally, the call chain looks like:
   * at com.nec.ve.VeSerializer$VeSerializationStream.writeObject(VeSerializer.scala:64)
   * at org.apache.spark.serializer.SerializationStream.writeValue(Serializer.scala:134)
   * at org.apache.spark.storage.DiskBlockObjectWriter.write(DiskBlockObjectWriter.scala:249)
   * at org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriter.write(BypassMergeSortShuffleWriter.java:158)
   * at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
   *
   * For our use case, Spark only writes Ints and VeColBatch for serialization.
   */
  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    t match {
      case i: java.lang.Integer =>
        dataOutputStream.writeInt(IntTag)
        dataOutputStream.writeInt(i)
        this
      case v: VeColBatch =>
        dataOutputStream.writeInt(CbTag)
        v.serializeToStream(dataOutputStream)
        this
      case v: BytesOnly =>
        dataOutputStream.writeInt(MixedCbTagColBatch)
        println(s"Will write ${v.size} (${v.bytes.size} as MixedBatch")
        dataOutputStream.writeInt(v.size)
        dataOutputStream.write(v.bytes)
        this
      case v: ColBatchWrapper =>
        dataOutputStream.writeInt(MixedCbTagColBatch)
//        println(s"Will write ${v.size} (${v.bytes.size} as MixedBatch")
        /** for reading out as byte array */

        import SparkCycloneExecutorPlugin.cycloneMetrics
        cycloneMetrics.measureRunningTime {
          dataOutputStream.writeInt(v.veColBatch.serializeToStreamSize)
          val startSize = dataOutputStream.size()
          v.veColBatch.serializeToStream(dataOutputStream)
          dataOutputStream.flush()
          val endSize = dataOutputStream.size()
          val diff = endSize - startSize
          require(
            diff == v.veColBatch.serializeToStreamSize,
            s"Written ${diff} bytes, expected ${v.veColBatch.serializeToStreamSize}"
          )
        }(cycloneMetrics.registerSerializationTime)
        this
      case other =>
        sys.error(s"Not supported here to write item of type ${other.getClass.getCanonicalName}")
    }
  }

  override def flush(): Unit = {
    dataOutputStream.flush()
  }

  override def close(): Unit = {
    dataOutputStream.close()
  }
}
