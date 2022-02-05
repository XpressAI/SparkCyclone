package com.nec.ve

import com.nec.ve.VeSerializer.VeSerializedContainer.VeColBatchToSerialize
import com.nec.ve.VeSerializer.{VeSerializedContainer, VeSerializerInstance}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{
  DeserializationStream,
  JavaSerializer,
  SerializationStream,
  Serializer,
  SerializerInstance
}

import java.io.{Externalizable, InputStream, ObjectInput, ObjectOutput, OutputStream}
import java.nio.ByteBuffer
import scala.reflect.ClassTag

class VeSerializer(conf: SparkConf) extends Serializer with Externalizable {
  private val js = new JavaSerializer(conf)

  protected def this() = this(new SparkConf())

  override def newInstance(): SerializerInstance = new VeSerializerInstance(js.newInstance())

  override def writeExternal(out: ObjectOutput): Unit = js.writeExternal(out)

  override def readExternal(in: ObjectInput): Unit = js.readExternal(in)
}

object VeSerializer {

  class VeSerializerInstance(parent: SerializerInstance) extends SerializerInstance with Logging {
    override def serialize[T: ClassTag](t: T): ByteBuffer =
      sys.error("This should not be reached")

    override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
      sys.error("This should not be reached")

    override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
      sys.error("This should not be reached")

    override def serializeStream(s: OutputStream): SerializationStream = new VeSerializationStream(
      s
    )

    override def deserializeStream(s: InputStream): DeserializationStream =
      new VeDeserializationStream(s)

  }

  sealed trait VeSerializedContainer
  object VeSerializedContainer {
    final case class VeColBatchToSerialize(veColBatch: VeColBatch) extends VeSerializedContainer
    final case class JavaLangInteger(i: Int) extends VeSerializedContainer

    def unapply(any: Any): Option[VeSerializedContainer] = PartialFunction.condOpt(any) {
      case i: java.lang.Integer =>
        JavaLangInteger(i)
      case vb: VeColBatchToSerialize =>
        vb
    }
  }

  class VeSerializationStream(out: OutputStream) extends SerializationStream with Logging {
    def writeContainer(e: VeSerializedContainer): VeSerializationStream = ???

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

    override def flush(): Unit = out.flush()

    override def close(): Unit = out.close()
  }

  class VeDeserializationStream(in: InputStream) extends DeserializationStream with Logging {

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

    def readOut(): VeSerializedContainer = ???

    override def close(): Unit = in.close()
  }
}
