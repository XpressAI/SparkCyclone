package com.nec.spark

import com.eed3si9n.expecty.Expecty.expect
import com.nec.cmake.CNativeEvaluator
import com.nec.spark.ReadFullCSVSpec.SampleRow
import com.nec.spark.ReadFullCSVSpec.samplePartedCsv
import com.nec.spark.planning.NativeCsvExec.NativeCsvStrategy
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec

import java.nio.file.Paths

object ReadFullCSVSpec {

  final case class SampleRow(a: Double, b: Double, c: Double)
  val targetPath = Paths.get("target").toAbsolutePath
  val samplePartedCsv = targetPath.resolve("sample.csv").toString

}

final class ReadFullCSVSpec extends AnyFreeSpec with BeforeAndAfter with SparkAdditions {
  "We can write and read back a .csv.gz collection via Hadoop" ignore withSparkSession2(identity) {
    sparkSession =>
      info(
        "This shows that we can read these files from hdfs, and then should be able to read them as a whole and push to the VE."
      )
      info(
        "Currently we get a String however to make a Byte array is very straightforward, and will bring good performance gains."
      )
      import sparkSession.sqlContext.implicits._
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

  "Execute a read of files via SQL, to see what plans it gives us" in withSparkSession2(
    _.config(CODEGEN_FALLBACK.key, value = false)
      .withExtensions(sse =>
        sse.injectPlannerStrategy(sparkSession => new NativeCsvStrategy(CNativeEvaluator))
      )
  ) { sparkSession =>
    import sparkSession.implicits._

    val schema = StructType(
      Array(
        StructField("a", DoubleType),
        StructField("b", DoubleType),
        StructField("c", DoubleType)
      )
    )

    val sumDs = sparkSession.sqlContext.read
      .schema(schema)
      .csv(samplePartedCsv)
      .as[SampleRow]
      .selectExpr("SUM(a)")
      .as[Double]

    val totalSum = sumDs
      .collect()

    expect(sumDs.executionPlan.toString().contains("NativeCsv"), totalSum.head == 12)
  }
}
