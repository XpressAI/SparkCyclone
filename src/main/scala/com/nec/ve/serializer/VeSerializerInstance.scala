package com.nec.ve.serializer

import com.nec.spark.SparkCycloneExecutorPlugin
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, SerializerInstance}

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer
import scala.reflect.ClassTag

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
      SparkCycloneExecutorPlugin.source,
      SparkCycloneExecutorPlugin.veMetrics
    )

  override def deserializeStream(s: InputStream): DeserializationStream =
    new VeDeserializationStream(s)(
      SparkCycloneExecutorPlugin.veProcess,
      SparkCycloneExecutorPlugin.source,
      SparkCycloneExecutorPlugin.veMetrics
    )

}
