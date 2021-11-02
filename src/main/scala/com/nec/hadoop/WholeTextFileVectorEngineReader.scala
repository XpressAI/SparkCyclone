/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.nec.hadoop

import com.google.common.io.ByteStreams
import com.google.common.io.Closeables
import org.apache.spark.input.PortableDataStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.{Configurable => HConfigurable}
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit

trait Configurable extends HConfigurable {
  private var conf: Configuration = _
  def setConf(c: Configuration): Unit = {
    conf = c
  }
  def getConf: Configuration = conf
}

object WholeTextFileVectorEngineReader {
  final case class VectorEngineRecord(pointer: Long, length: Int) {
    def getRawData: Array[Byte] = Array.empty
  }
}

/**
 * A org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader CombineFileRecordReader
 * that can pass Hadoop Configuration to org.apache.hadoop.conf.Configurable Configurable
 * RecordReaders.
 */
class ConfigurableCombineFileRecordReader[K, V](
  split: InputSplit,
  context: TaskAttemptContext,
  recordReaderClass: Class[_ <: RecordReader[K, V] with HConfigurable]
) extends CombineFileRecordReader[K, V](
    split.asInstanceOf[CombineFileSplit],
    context,
    recordReaderClass
  )
  with Configurable {

  override def initNextRecordReader(): Boolean = {
    val r = super.initNextRecordReader()
    if (r) {
      if (getConf != null) {
        this.curReader.asInstanceOf[HConfigurable].setConf(getConf)
      }
    }
    r
  }

  override def setConf(c: Configuration): Unit = {
    super.setConf(c)
    if (this.curReader != null) {
      this.curReader.asInstanceOf[HConfigurable].setConf(c)
    }
  }
}
