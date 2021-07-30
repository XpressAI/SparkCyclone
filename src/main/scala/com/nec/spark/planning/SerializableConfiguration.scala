package com.nec.spark.planning

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.conf.Configuration

import org.apache.spark.util.Utils

class SerializableConfiguration(var conf: Configuration) extends Serializable {

  def this() {
  this(new Configuration())
}

  def get(): Configuration = conf

  private def writeObject (out: java.io.ObjectOutputStream): Unit = {
  conf.write(out)
}

  private def readObject (in: java.io.ObjectInputStream): Unit = {
  conf = new Configuration()
  conf.readFields(in)
}

  private def readObjectNoData(): Unit = {
  conf = new Configuration()
}
}