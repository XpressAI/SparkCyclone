package com.nec.ve.serializer

import org.apache.spark.SparkConf
import org.apache.spark.serializer.{Serializer, SerializerInstance}

class VeSerializer(conf: SparkConf, cleanUpInput: Boolean) extends Serializer with Serializable {
  override def newInstance(): SerializerInstance = new VeSerializerInstance(cleanUpInput)
}
