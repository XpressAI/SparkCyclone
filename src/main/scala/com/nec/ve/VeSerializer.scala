package com.nec.ve

import com.nec.ve.VeSerializer.VeSerializerInstance
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
    override def serialize[T](t: T)(implicit evidence$1: ClassTag[T]): ByteBuffer = {
      logError(s"Serializing: ${evidence$1} ${t.getClass.getCanonicalName}")
      parent.serialize(t)
    }

    override def deserialize[T](bytes: ByteBuffer)(implicit evidence$2: ClassTag[T]): T = {
      logError(s"Deserializing: ${evidence$2}")
      parent.deserialize(bytes)
    }

    override def deserialize[T](bytes: ByteBuffer, loader: ClassLoader)(implicit
      evidence$3: ClassTag[T]
    ): T = {
      logError(s"Deserializing: ${evidence$3}")
      parent.deserialize(bytes, loader)
    }

    override def serializeStream(s: OutputStream): SerializationStream = new VeSerializationStream(
      parent.serializeStream(s)
    )

    override def deserializeStream(s: InputStream): DeserializationStream =
      new VeDeserializationStream(parent.deserializeStream(s))

  }

  class VeSerializationStream(parent: SerializationStream)
    extends SerializationStream
    with Logging {
    override def writeObject[T](t: T)(implicit evidence$4: ClassTag[T]): SerializationStream = {
      logError(s"Will write object ${t.getClass}; ${evidence$4}")
      new Exception().printStackTrace()
      parent.writeObject(t)
    }

    override def flush(): Unit = parent.flush()

    override def close(): Unit = parent.close()
  }

  class VeDeserializationStream(parent: DeserializationStream)
    extends DeserializationStream
    with Logging {
    override def readObject[T]()(implicit evidence$8: ClassTag[T]): T = {
      logError(s"ReadObj ${evidence$8}")
      new Exception().printStackTrace()
      parent.readObject[T]()
    }

    override def close(): Unit = parent.close()
  }
}
