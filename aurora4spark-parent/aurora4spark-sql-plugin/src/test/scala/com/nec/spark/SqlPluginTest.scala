package com.nec.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.freespec.AnyFreeSpec

object SqlPluginTest {}
final class SqlPluginTest extends AnyFreeSpec {

  "It is not launched if not specified" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("local-test")
    val sparkContext = new SparkContext(conf)

    try {
      assert(
        !Aurora4SparkDriver.launched,
        "Expect the driver to have not been launched"
      )
    } finally sparkContext.stop()
  }

  "It is launched if specified" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("local-test")
    conf.set(
      "spark.plugins",
      classOf[SqlPlugin].getName
    )
    val sparkContext = new SparkContext(conf)
    try {
      assert(
        Aurora4SparkDriver.launched,
        "Expect the driver to have been launched"
      )
    } finally sparkContext.stop()
  }

  "We can run a Spark-SQL job" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      val result = sparkSession.sql("SELECT 1 + 2").as[Int].collect().toList
      assert(result == List(3))
    } finally sparkSession.close()
  }

  "We can get an execution plan for a sum of rows and dissect the programmatic structure" in {
    info(
      """
        |We do this so that we know exactly what we need to optimize/rewrite for GPU processing
        |
        |We will get there more quickly by doing this sort of use case based reverse engineering.
        |""".stripMargin
    )
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set(
      "spark.sql.extensions",
      classOf[SparkPlanSavingPlugin].getCanonicalName
    )
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      Seq(1, 2, 3)
        .toDS()
        .createOrReplaceTempView("nums")

      val result =
        sparkSession.sql("SELECT SUM(value) FROM nums").as[BigDecimal].head()

      assert(
        SparkPlanSavingPlugin.savedSparkPlan.getClass.getCanonicalName == "org.apache.spark.sql.execution.aggregate.HashAggregateExec"
      )
      SparkPlanSavingPlugin.savedSparkPlan match {
        case first @ HashAggregateExec(
              requiredChildDistributionExpressions,
              groupingExpressions,
              aggregateExpressions,
              aggregateAttributes,
              initialInputBufferOffset,
              resultExpressions,
              child
            ) =>
          info(s"First root of the plan: ${first}")
          assert(
            child.getClass.getCanonicalName == "org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"
          )
          child match {
            case second @ org.apache.spark.sql.execution.exchange
                  .ShuffleExchangeExec(
                    outputPartitioning,
                    child2,
                    shuffleOrigin
                  ) =>
              info(s"Second root of the plan: ${second}")
              assert(
                child2.getClass.getCanonicalName == "org.apache.spark.sql.execution.aggregate.HashAggregateExec"
              )
              child2 match {
                case third @ org.apache.spark.sql.execution.aggregate
                      .HashAggregateExec(
                        requiredChildDistributionExpressions,
                        groupingExpressions,
                        aggregateExpressions,
                        aggregateAttributes,
                        initialInputBufferOffset,
                        resultExpressions,
                        child3
                      ) =>
                  info(s"Third root of the plan: ${third}")
                  assert(
                    child3.getClass.getCanonicalName == "org.apache.spark.sql.execution.LocalTableScanExec"
                  )
                  child3 match {
                    case fourth @ LocalTableScanExec(output, rows) =>
                      info(s"Fourth root of the plan: ${fourth}")
                      assert(output.length == 1, "There is only 1 column")
                      assert(rows.length == 3, "There are 3 rows")
                  }
              }
          }
      }

      assert(result == BigDecimal(6))
    } finally sparkSession.close()
  }

  "We can do a pretend plug-in that will return 6" in {
    info(
      """
      This enables us to be confident this is the right place to do the rewriting.
      The significance of this is that from here, we will be able to compare expected results with actual results after we apply the plug-in
      """
    )
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set(
      "spark.sql.extensions",
      classOf[DummyShortCircuitSqlPlugin].getCanonicalName
    )
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._

      DummyShortCircuitSqlPlugin.applyShortCircuit = false
      Seq(1, 2, 3)
        .toDS()
        .createOrReplaceTempView("nums")
      val sumDataSet =
        sparkSession.sql("SELECT SUM(value) FROM nums").as[BigDecimal]

      DummyShortCircuitSqlPlugin.applyShortCircuit = true
      sumDataSet.explain(true)
      val result = sumDataSet.head()

      assert(result == BigDecimal(6))
    } finally sparkSession.close()
  }

  /** this test is ignored because it's currently failing as our computation engine returns only a static/stubbed value. */
  "We can do a pretend plug-in that will return 6, however it should return 10" taggedAs AcceptanceTest in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set(
      "spark.sql.extensions",
      classOf[DummyShortCircuitSqlPlugin].getCanonicalName
    )
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      DummyShortCircuitSqlPlugin.applyShortCircuit = false

      Seq(1, 2, 3, 4)
        .toDS()
        .createOrReplaceTempView("nums")

      val sumDataSet =
        sparkSession.sql("SELECT SUM(value) FROM nums").as[BigDecimal]

      DummyShortCircuitSqlPlugin.applyShortCircuit = true
      sumDataSet.explain(true)
      val result = sumDataSet.head()

      assert(result == BigDecimal(10))
    } finally sparkSession.close()
  }

}
