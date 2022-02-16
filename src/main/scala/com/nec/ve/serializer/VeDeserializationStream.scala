package com.nec.ve.serializer

import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.ve.{VeColBatch, VeProcess}
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.nec.ve.serializer.DualBatchOrBytes.BytesOnly
import com.nec.ve.serializer.VeDeserializationStream.DeserStreamed
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.DeserializationStream

import java.io.{DataInputStream, EOFException, InputStream}
import scala.reflect.ClassTag

class VeDeserializationStream(in: InputStream)(implicit
  veProcess: VeProcess,
  veColVectorSource: VeColVectorSource
) extends DeserializationStream
  with Logging {
  logDebug(s"Inputting from ==> ${in}; ${in.getClass}")
  val dataInputStream = new DataInputStream(in)

  /**
   * Generally, the call chain looks like:
   *        at com.nec.ve.VeSerializer$VeDeserializationStream.readObject(VeSerializer.scala:78)
   *        at org.apache.spark.serializer.DeserializationStream.readKey(Serializer.scala:156) (or readValue)
   *        at org.apache.spark.serializer.DeserializationStream$$anon$2.getNext(Serializer.scala:188)
   *        at org.apache.spark.serializer.DeserializationStream$$anon$2.getNext(Serializer.scala:185)
   *        at org.apache.spark.util.NextIterator.hasNext(NextIterator.scala:73)
   *        at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:488)
   *        at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
   *        at org.apache.spark.util.CompletionIterator.hasNext(CompletionIterator.scala:31)
   *        at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)
   *        at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
   *        at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:489)
   *        at scala.collection.Iterator.isEmpty(Iterator.scala:385)
   *        at scala.collection.Iterator.isEmpty$(Iterator.scala:385)
   *        at scala.collection.AbstractIterator.isEmpty(Iterator.scala:1429)
   *        at scala.collection.TraversableOnce.max(TraversableOnce.scala:233)
   *        at scala.collection.TraversableOnce.max$(TraversableOnce.scala:232)
   *        at scala.collection.AbstractIterator.max(Iterator.scala:1429)
   *        at com.nec.ve.VERDDSpec$.$anonfun$exchangeBatches$7(VERDDSpec.scala:120)
   *
   * For our use case, Spark only writes Ints and VeColBatch for serialization.
   */
  override def readObject[T: ClassTag](): T =
    in.synchronized {
      dataInputStream.readInt() match {
        case IntTag =>
          dataInputStream.readInt()
        case CbTag =>
          import com.nec.ve.VeProcess.OriginalCallingContext.Automatic._
          VeColBatch.fromStream(dataInputStream)
        case MixedCbTagColBatch =>
          val theSize = dataInputStream.readInt()
          import SparkCycloneExecutorPlugin.metrics
          if (DeserStreamed) {
            import com.nec.ve.VeProcess.OriginalCallingContext.Automatic._
            metrics.measureRunningTime {
              DualBatchOrBytes.ColBatchWrapper(VeColBatch.fromStream(dataInputStream))
            }(metrics.registerDeserializationTime)
          } else {
            metrics.measureRunningTime {
              val byteArray = Array.fill[Byte](theSize)(-1)
              dataInputStream.readFully(byteArray)
              new BytesOnly(byteArray, theSize): DualBatchOrBytes
            }(metrics.registerDeserializationTime)
          }
        case -1 =>
          throw new EOFException()
        case other =>
          sys.error(
            s"Unexpected tag: ${other}, expected only ${IntTag}, ${CbTag} or ${MixedCbTagColBatch}"
          )
      }
    }.asInstanceOf[T]

  override def close(): Unit =
    dataInputStream.close()
}

object VeDeserializationStream {
//  val DeserStreamed = true
  val DeserStreamed = false
}
