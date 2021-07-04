package com.nec.spark

import com.eed3si9n.expecty.Expecty.expect
import com.nec.spark.ReadFullCSVSpec.SampleRow
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec

import java.nio.file.Paths

object ReadFullCSVSpec {
  final case class SampleRow(a: Double, b: Double, c: Double)
}

final class ReadFullCSVSpec extends AnyFreeSpec with BeforeAndAfter with SparkAdditions {
  "We can write and read back a .csv.gz collection via Hadoop" in withSparkSession2(identity) {
    sparkSession =>
      info(
        "This shows that we can read these files from hdfs, and then should be able to read them as a whole and push to the VE."
      )
      info(
        "Currently we get a String however to make a Byte array is very straightforward, and will bring good performance gains."
      )
      import sparkSession.sqlContext.implicits._
      val targetPath = Paths.get("target").toAbsolutePath
      val samplePartedCsv = targetPath.resolve("sample.csv").toString
      List[SampleRow](
        SampleRow(1, 2, 3),
        SampleRow(4, 5, 6),
        SampleRow(5, 4, 3),
        SampleRow(2, 1, -1)
      )
        .toDF()
        .repartition(numPartitions = 3)
        .write
        .format("csv")
        .option("header", "true")
        .mode("overwrite")
        .option("compression", "gzip")
        .save(samplePartedCsv)
      val listOfPairs = sparkSession.sparkContext
        .wholeTextFiles(samplePartedCsv)
        .collect()
        .toList

      expect(listOfPairs.size == 3, listOfPairs.exists(_._2.contains("5.0,4.0,3.0")))
  }
}
