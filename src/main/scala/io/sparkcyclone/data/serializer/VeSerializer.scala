package io.sparkcyclone.data.serializer

import org.apache.spark.SparkConf
import org.apache.spark.serializer.{Serializer, SerializerInstance}

class VeSerializer(conf: SparkConf) extends Serializer with Serializable {
  override def newInstance: SerializerInstance = new VeSerializerInstance
}
