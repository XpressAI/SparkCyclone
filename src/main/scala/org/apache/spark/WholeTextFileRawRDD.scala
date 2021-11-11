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
