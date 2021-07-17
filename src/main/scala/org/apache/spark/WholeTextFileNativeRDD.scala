package org.apache.spark

import com.nec.hadoop.WholeTextFileVectorEngineInputFormat
import com.nec.hadoop.WholeTextFileVectorEngineReader.VectorEngineRecord
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.rdd.RDD

object WholeTextFileNativeRDD {
  implicit final class RichSparkContext(sparkContext: SparkContext) {
    def wholeNativeTextFiles(
      path: String,
      minPartitions: Int = sparkContext.defaultMinPartitions
    ): RDD[(String, VectorEngineRecord)] = sparkContext.withScope {
      val job = Job.getInstance(sparkContext.hadoopConfiguration)
      // Use setInputPaths so that wholeTextFiles aligns with hadoopFile/textFile in taking
      // comma separated files as input. (see SPARK-7155)
      FileInputFormat.setInputPaths(job, path)
      val updateConf = job.getConfiguration
      new WholeTextFileVectorEngineRDD(
        sparkContext,
        classOf[WholeTextFileVectorEngineInputFormat],
        classOf[Text],
        classOf[VectorEngineRecord],
        updateConf,
        minPartitions
      ).map(record => (record._1.toString, record._2)).setName(path)
    }
  }
}
