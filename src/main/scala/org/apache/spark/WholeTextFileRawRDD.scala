package org.apache.spark
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.input.WholeTextFileInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.WholeTextFileRDD

object WholeTextFileRawRDD {
  implicit final class RichSparkContext(sparkContext: SparkContext) {
    def wholeRawTextFiles(
      path: String,
      minPartitions: Int = sparkContext.defaultMinPartitions
    ): RDD[(String, Text)] = sparkContext.withScope {
      val job = Job.getInstance(sparkContext.hadoopConfiguration)
      // Use setInputPaths so that wholeTextFiles aligns with hadoopFile/textFile in taking
      // comma separated files as input. (see SPARK-7155)
      FileInputFormat.setInputPaths(job, path)
      val updateConf = job.getConfiguration
      new WholeTextFileRDD(
        sparkContext,
        classOf[WholeTextFileInputFormat],
        classOf[Text],
        classOf[Text],
        updateConf,
        minPartitions
      ).map(record => (record._1.toString, record._2)).setName(path)
    }
  }
}
