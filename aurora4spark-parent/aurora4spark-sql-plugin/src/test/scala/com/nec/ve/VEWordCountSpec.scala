package com.nec.ve

import com.nec.spark.SparkAdditions
import com.nec.spark.AuroraSqlPlugin
import com.nec.spark.planning.WordCountPlanner
import com.nec.spark.planning.WordCountPlanner.WordCounter
import com.nec.ve.VEWordCountSpec.WordCountQuery
import org.apache.spark.sql.execution.PlanExtractor.DatasetPlanExtractor
import org.apache.spark.sql.internal.SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED
import org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec

object VEWordCountSpec {
  val WordCountQuery =
    "SELECT word, count(word) AS count FROM words GROUP by word HAVING count > 1 ORDER by count DESC LIMIT 10"
}
final class VEWordCountSpec extends AnyFreeSpec with BeforeAndAfter with SparkAdditions {

  "Word-count on the VE" in withSparkSession(
    _.set("spark.plugins", classOf[AuroraSqlPlugin].getCanonicalName)
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
        .sql(WordCountQuery)
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
