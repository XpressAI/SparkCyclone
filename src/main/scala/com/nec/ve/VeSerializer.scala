package com.nec.ve

import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.ve.VeSerializer.VeSerializedContainer.{
  CbTag,
  IntTag,
  VeColBatchesDeserialized,
  VeColBatchesToSerialize
}
import com.nec.ve.VeSerializer.VeSerializerInstance
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.serializer._

import java.io._
import java.nio.ByteBuffer
import scala.reflect.ClassTag

class VeSerializer(conf: SparkConf, cleanUpInput: Boolean) extends Serializer with Serializable {
  override def newInstance(): SerializerInstance = new VeSerializerInstance(cleanUpInput)
}

object VeSerializer {

  class VeSerializerInstance(cleanUpInput: Boolean) extends SerializerInstance with Logging {
    override def serialize[T: ClassTag](t: T): ByteBuffer =
      sys.error("This should not be reached")

    override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
      sys.error("This should not be reached")

    override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
      sys.error("This should not be reached")

    override def serializeStream(s: OutputStream): SerializationStream =
      new VeSerializationStream(s)(
        SparkCycloneExecutorPlugin.veProcess,
        SparkCycloneExecutorPlugin.source
      )

    override def deserializeStream(s: InputStream): DeserializationStream =
      new VeDeserializationStream(s)(
        SparkCycloneExecutorPlugin.veProcess,
        SparkCycloneExecutorPlugin.source
      )

  }

  sealed trait VeSerializedContainer {
    def tag: Int

  }

  object VeSerializedContainer {
    val CbTag = 91
    val IntTag = 92
    sealed trait VeColBatchHolder extends VeSerializedContainer {}
    final case class VeColBatchesToSerialize(veColBatch: VeColBatch) extends VeColBatchHolder {
      override def tag: Int = CbTag
    }
    final case class VeColBatchesDeserialized(veColBatch: VeColBatch) extends VeColBatchHolder {
      override def tag: Int = CbTag
    }
    final case class JavaLangInteger(i: Int) extends VeSerializedContainer {
      override def tag: Int = IntTag
    }

    def unapply(any: Any): Option[VeSerializedContainer] = PartialFunction.condOpt(any) {
      case i: java.lang.Integer =>
        JavaLangInteger(i)
      case vb: VeColBatchesToSerialize =>
        vb
    }
  }

  class VeSerializationStream(out: OutputStream)(implicit
    veProcess: VeProcess,
    veColVectorSource: VeColVectorSource
  ) extends SerializationStream
    with Logging {
    val dataOutputStream = new DataOutputStream(out)
    logDebug(s"Outputting to ==> ${out}; ${out.getClass}")
    def writeContainer(e: VeSerializedContainer): VeSerializationStream = {
      out.synchronized {
        println(s"Writing thread ==> ${Thread.currentThread()}")
        out.write(e.tag)

        e match {
          case VeColBatchesToSerialize(veColBatch) =>
            val serialized = veColBatch.serializeToBytes()
            dataOutputStream.writeInt(serialized.length)
            dataOutputStream.write(serialized)
          case VeSerializedContainer.JavaLangInteger(i) => dataOutputStream.writeInt(i)
          case VeColBatchesDeserialized(_) =>
            sys.error("Should not get to this state.")
        }

        this
      }
    }

    /**
     * Generally, the call chain looks like:
     * at com.nec.ve.VeSerializer$VeSerializationStream.writeObject(VeSerializer.scala:64)
     * at org.apache.spark.serializer.SerializationStream.writeValue(Serializer.scala:134)
     * at org.apache.spark.storage.DiskBlockObjectWriter.write(DiskBlockObjectWriter.scala:249)
     * at org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriter.write(BypassMergeSortShuffleWriter.java:158)
     * at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
     */
    override def writeObject[T: ClassTag](t: T): SerializationStream = {
      t match {
        case VeSerializedContainer(cont) => writeContainer(cont)
        case other =>
          sys.error(s"Not supported here to write item of type ${other.getClass.getCanonicalName}")
      }
    }

    override def flush(): Unit = {
      dataOutputStream.flush()
      out.flush()
    }

    override def close(): Unit = {
      dataOutputStream.close()
      out.close()
    }
  }

  class VeDeserializationStream(in: InputStream)(implicit
    veProcess: VeProcess,
    veColVectorSource: VeColVectorSource
  ) extends DeserializationStream
    with Logging {
    logDebug(s"Inputting from ==> ${in}; ${in.getClass}")
    val din = new DataInputStream(in)

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
     */
    override def readObject[T: ClassTag](): T =
      readOut().asInstanceOf[T]

    def readOut(): VeSerializedContainer = {
      in.synchronized {
        println(s"Reading thread ==> ${Thread.currentThread()}")
        din.read() match {
          case VeSerializedContainer.IntTag =>
            VeSerializedContainer.JavaLangInteger(din.readInt())
          case VeSerializedContainer.CbTag =>
            val size = din.readInt()
            val arr = Array.fill[Byte](size)(-1)
            din.read(arr)
            println(arr.toList)
            VeSerializedContainer.VeColBatchesDeserialized(VeColBatch.readFromBytes(arr))
          case -1 =>
            throw new EOFException()
          case other =>
            sys.error(s"Unexpected tag: ${other}, expected only ${IntTag} or ${CbTag}")
        }
      }
    }

    override def close(): Unit =
      try din.close()
      finally in.close()
  }
}
