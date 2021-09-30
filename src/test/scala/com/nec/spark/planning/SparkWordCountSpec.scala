package com.nec.spark.planning

import com.nec.spark.SampleTestData
import com.nec.spark.SparkAdditions
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.execution.SparkPlan
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec

final class SparkWordCountSpec extends AnyFreeSpec with BeforeAndAfter with SparkAdditions {

  "We can do a Word count from memory and split words" in withSparkSession(identity) {
    sparkSession =>
      import sparkSession.implicits._
      List("This is", "some test", "of some stuff", "that always is").toDF
        .selectExpr("explode(split(value, ' ')) as word")
        .createOrReplaceTempView("words")

      val wordCountQuery =
        sparkSession
          .sql(
            "SELECT word, count(word) AS count FROM words GROUP by word HAVING count > 1 ORDER by count DESC"
          )
          .as[(String, BigInt)]

      assert(wordCountQuery.collect().toList.toMap == Map("some" -> 2, "is" -> 2))
  }

  "We can do a Word count from a text file" in withSparkSession(identity) { sparkSession =>
    import sparkSession.implicits._
    sparkSession.read
      .textFile(SampleTestData.SampleStrCsv.toString)
      .selectExpr("explode(split(value, '[, ]')) as word")
      .createOrReplaceTempView("words")

    val wordCountQuery =
      sparkSession
        .sql(
          "SELECT word, count(word) AS count FROM words GROUP by word HAVING count > 1 ORDER by count DESC LIMIT 10"
        )
        .as[(String, BigInt)]
    assert(wordCountQuery.collect().toList.toMap == Map("some" -> 2, "is" -> 2))
  }

  "We can count-distinct some pre-split strings for a Word count" in withSparkSession(identity) {
    sparkSession =>
      import sparkSession.implicits._

      List("a", "ab", "bc", "ab")
        .toDS()
        .withColumnRenamed("value", "word")
        .createOrReplaceTempView("words")

      val wordCountQuery =
        sparkSession
          .sql(
            "SELECT word, count(word) AS count FROM words GROUP by word HAVING count > 1 ORDER by count DESC LIMIT 10"
          )
          .as[(String, BigInt)]

      val result = wordCountQuery.collect().toList.toMap
      assert(result == Map("ab" -> 2))

  }

  private def collectSparkPlan[U: Encoder](sparkPlan: SparkPlan): Array[U] = {
    import scala.collection.JavaConverters._
    sparkPlan.sqlContext
      .createDataFrame(
        rows = sparkPlan.executeCollectPublic().toList.asJava,
        schema = sparkPlan.schema
      )
      .as[U]
      .collect()
  }

}
