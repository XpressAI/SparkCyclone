package com.nec.ve.serializer

import com.nec.spark.SparkCycloneExecutorPlugin
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, SerializerInstance}
import scala.reflect.ClassTag
import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

class VeSerializerInstance extends SerializerInstance {
  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    sys.error("This should not be reached")
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    sys.error("This should not be reached")
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    sys.error("This should not be reached")
  }

  override def serializeStream(in: OutputStream): SerializationStream = {
    new VeSerializationStream(in)(
      SparkCycloneExecutorPlugin.veProcess,
      SparkCycloneExecutorPlugin.veMetrics
    )
  }

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new VeDeserializationStream(in)(
      SparkCycloneExecutorPlugin.vectorEngine,
      SparkCycloneExecutorPlugin.veMetrics
    )
  }
}
