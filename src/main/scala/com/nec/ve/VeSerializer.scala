package com.nec.ve

import com.nec.ve.VeSerializer.VeSerializerInstance
import org.apache.spark.SparkConf
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

  class VeSerializerInstance(parent: SerializerInstance) extends SerializerInstance {
    override def serialize[T](t: T)(implicit evidence$1: ClassTag[T]): ByteBuffer = {
      println(s"Serializing: ${evidence$1}")
      parent.serialize(t)
    }

    override def deserialize[T](bytes: ByteBuffer)(implicit evidence$2: ClassTag[T]): T = {
      println(s"Deserializing: ${evidence$2}")
      parent.deserialize(bytes)
    }

    override def deserialize[T](bytes: ByteBuffer, loader: ClassLoader)(implicit
      evidence$3: ClassTag[T]
    ): T = {
      println(s"Deserializing: ${evidence$3}")
      parent.deserialize(bytes, loader)
    }

    override def serializeStream(s: OutputStream): SerializationStream = parent.serializeStream(s)

    override def deserializeStream(s: InputStream): DeserializationStream =
      parent.deserializeStream(s)
  }
}
