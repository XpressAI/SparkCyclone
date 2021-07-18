package org.apache.spark

import com.nec.hadoop.Configurable
import com.nec.hadoop.WholeTextFileVectorEngineInputFormat
import com.nec.hadoop.WholeTextFileVectorEngineReader.VectorEngineRecord
import org.apache.hadoop.mapreduce.task.JobContextImpl
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.spark.rdd.NewHadoopPartition

class WholeTextFileVectorEngineRDD(
  sc: SparkContext,
  inputFormatClass: Class[_ <: WholeTextFileVectorEngineInputFormat],
  keyClass: Class[Text],
  valueClass: Class[VectorEngineRecord],
  conf: Configuration,
  minPartitions: Int
) extends NewHadoopRDD[Text, VectorEngineRecord](sc, inputFormatClass, keyClass, valueClass, conf) {

  override def getPartitions: Array[Partition] = {
    val conf = getConf
    // setMinPartitions below will call FileInputFormat.listStatus(), which can be quite slow when
    // traversing a large number of directories and files. Parallelize it.
    conf.setIfUnset(
      FileInputFormat.LIST_STATUS_NUM_THREADS,
      Runtime.getRuntime.availableProcessors().toString
    )
    val inputFormat = inputFormatClass.getConstructor().newInstance()
    inputFormat match {
      case configurable: Configurable =>
        configurable.setConf(conf)
      case _ =>
    }
    val jobContext = new JobContextImpl(conf, jobId)
    inputFormat.setMinPartitions(jobContext, minPartitions)
    val rawSplits = inputFormat.getSplits(jobContext).toArray
    val result = new Array[Partition](rawSplits.size)
    for (i <- 0 until rawSplits.size) {
      result(i) = new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
    }
    result
  }
}
