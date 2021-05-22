package com.nec.spark.agile

import com.nec.spark.{AcceptanceTest, AuroraSqlPlugin, LocalVeoExtension}
import com.nec.spark.agile.WordCountPlanner.WordCounter
import com.nec.spark.agile.WordCountSpec.withArrowStringVector
import org.apache.arrow.vector.{FieldVector, VarCharVector}
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.execution.PlanExtractor.DatasetPlanExtractor
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf.{
  COLUMN_VECTOR_OFFHEAP_ENABLED,
  WHOLESTAGE_CODEGEN_ENABLED
}
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec

import java.util

object WordCountSpec {
  def withArrowStringVector[T](
    stringBatch: Seq[String],
    schema: org.apache.arrow.vector.types.pojo.Schema
  )(f: VarCharVector => T): T = {
    import org.apache.arrow.memory.RootAllocator
    import org.apache.arrow.vector.VectorSchemaRoot
    val alloc = new RootAllocator(Integer.MAX_VALUE)
    try {
      val vcv = schema.findField("value").createVector(alloc).asInstanceOf[VarCharVector]
      vcv.allocateNew()
      try {
        val root = new VectorSchemaRoot(schema, util.Arrays.asList(vcv: FieldVector), 2)
        stringBatch.view.zipWithIndex.foreach { case (str, idx) =>
          vcv.setSafe(idx, str.getBytes("utf8"), 0, str.length)
        }
        vcv.setValueCount(stringBatch.length)
        root.setRowCount(stringBatch.length)
        f(vcv)
      } finally vcv.close()
    } finally alloc.close()
  }
}
final class WordCountSpec extends AnyFreeSpec with BeforeAndAfter with SparkAdditions {

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
      .textFile(ReferenceData.SampleTXT.toString)
      .selectExpr("explode(split(value, ' ')) as word")
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

  "We can translate a Count-Distinct with pre-split strings" in withSparkSession(
    _.set(WHOLESTAGE_CODEGEN_ENABLED.key, "false")
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
  ) { sparkSession =>
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

    val newPlan = WordCountPlanner.apply(
      wordCountQuery.extractQueryExecution.executedPlan,
      WordCounter.PlainJVM
    )

    assert(newPlan.toString.contains("CountPlanner"), newPlan.toString)
//    info(newPlan.toString)
    val result = collectSparkPlan[(String, Long)](newPlan).toList.toMap
    assert(result == Map("ab" -> 2))
  }

  "We can do a Word count from a text file with our transformation" in withSparkSession(
    _.set(WHOLESTAGE_CODEGEN_ENABLED.key, "false")
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
  ) { sparkSession =>
    import sparkSession.implicits._
    sparkSession.read
      .textFile(ReferenceData.SampleTXT.toString)
      .selectExpr("explode(split(value, ' ')) as word")
      .createOrReplaceTempView("words")

    val wordCountQuery =
      sparkSession
        .sql(
          "SELECT word, count(word) AS count FROM words GROUP by word HAVING count > 1 ORDER by count DESC LIMIT 10"
        )
        .as[(String, BigInt)]

    val newPlan = WordCountPlanner.apply(
      wordCountQuery.extractQueryExecution.executedPlan,
      WordCounter.PlainJVM
    )

    assert(newPlan.toString.contains("CountPlanner"), newPlan.toString)
//    info(newPlan.toString)
    val result = collectSparkPlan[(String, Long)](newPlan).toList.toMap
    assert(result == Map("some" -> 2, "is" -> 2))
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

  "Plain JVM word counter works" in {
    withArrowStringVector(
      stringBatch = List("a", "bb", "c", "a"),
      schema = org.apache.arrow.vector.types.pojo.Schema.fromJSON(
        """{"fields": [{"name": "value", "nullable" : true, "type": {"name": "utf8"}, "children": []}]}"""
      )
    ) { vcv =>
      assert(
        WordCounter.PlainJVM
          .countWords(vcv) == Map("a" -> 2, "bb" -> 1, "c" -> 1)
      )
    }
  }
  "Word count combiner works" in {
    assert(
      WordCounter.combine(Map("a" -> 1, "b" -> 1), Map("b" -> 1, "c" -> 3)) == Map(
        "a" -> 1,
        "b" -> 2,
        "c" -> 3
      )
    )
  }

  "Word-count on the VE" taggedAs (AcceptanceTest) in withSparkSession(
    _.set("spark.plugins", classOf[AuroraSqlPlugin].getCanonicalName)
      .set("spark.sql.extensions", classOf[LocalVeoExtension].getCanonicalName)
      .set(WHOLESTAGE_CODEGEN_ENABLED.key, "false")
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
  ) { sparkSession =>
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

    val planStr = wordCountQuery.extractQueryExecution.executedPlan.toString()

    val newPlan = WordCountPlanner.apply(
      wordCountQuery.extractQueryExecution.executedPlan,
      WordCounter.PlainJVM
    )

    assert(newPlan.toString.contains("CountPlanner"), newPlan.toString)
    assert(planStr.contains("CountPlanner"))

    val result = wordCountQuery.collect().toMap
    assert(result == Map("ab" -> 2))
  }

}
