package com.nec.hadoop

import com.nec.hadoop.WholeTextFileVectorEngineReader.VectorEngineRecord

import scala.collection.JavaConverters._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat

class WholeTextFileVectorEngineInputFormat
  extends CombineFileInputFormat[Text, VectorEngineRecord]
  with Configurable {

  override protected def isSplitable(context: JobContext, file: Path): Boolean = false

  override def createRecordReader(
    split: InputSplit,
    context: TaskAttemptContext
  ): RecordReader[Text, VectorEngineRecord] = {
    val reader =
      new ConfigurableCombineFileRecordReader(
        split,
        context,
        classOf[WholeTextFileVectorEngineReader]
      )
    reader.setConf(getConf)
    reader
  }

  /**
   * Allow minPartitions set by end-user in order to keep compatibility with old Hadoop API,
   * which is set through setMaxSplitSize
   */
  def setMinPartitions(context: JobContext, minPartitions: Int): Unit = {
    val files = listStatus(context).asScala
    val totalLen = files.map(file => if (file.isDirectory) 0L else file.getLen).sum
    val maxSplitSize = Math
      .ceil(
        totalLen * 1.0 /
          (if (minPartitions == 0) 1 else minPartitions)
      )
      .toLong

    // For small files we need to ensure the min split size per node & rack <= maxSplitSize
    val config = context.getConfiguration
    val minSplitSizePerNode = config.getLong(CombineFileInputFormat.SPLIT_MINSIZE_PERNODE, 0L)
    val minSplitSizePerRack = config.getLong(CombineFileInputFormat.SPLIT_MINSIZE_PERRACK, 0L)

    if (maxSplitSize < minSplitSizePerNode) {
      super.setMinSplitSizeNode(maxSplitSize)
    }

    if (maxSplitSize < minSplitSizePerRack) {
      super.setMinSplitSizeRack(maxSplitSize)
    }
    super.setMaxSplitSize(maxSplitSize)
  }
}
