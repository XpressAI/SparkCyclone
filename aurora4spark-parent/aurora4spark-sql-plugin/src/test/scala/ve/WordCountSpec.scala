package ve

import com.nec.spark.agile.{SparkAdditions, WordCountPlanner}
import com.nec.spark.{AuroraSqlPlugin, LocalVeoExtension}
import com.nec.spark.agile.WordCountPlanner.WordCounter
import org.apache.arrow.vector.{FieldVector, VarCharVector}
import org.apache.spark.sql.execution.PlanExtractor.DatasetPlanExtractor
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

  "Word-count on the VE" in withSparkSession(
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
