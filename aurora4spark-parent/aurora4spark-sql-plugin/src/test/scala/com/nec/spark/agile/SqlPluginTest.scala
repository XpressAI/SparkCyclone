package com.nec.spark.agile

import java.nio.file.Paths

import com.nec.spark.agile.BigDecimalSummer.ScalaSummer
import com.nec.spark.{AcceptanceTest, Aurora4SparkDriver, Aurora4SparkExecutorPlugin, SqlPlugin}
import org.apache.log4j.{Level, Logger}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.freespec.AnyFreeSpec

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.types.{DecimalType, DoubleType, StructField, StructType}

final class SqlPluginTest extends AnyFreeSpec with BeforeAndAfterAll with BeforeAndAfter {

  override protected def beforeAll(): Unit = {
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    super.beforeAll()
  }

  after{
    SparkSqlPlanExtension.rulesToApply.clear()
  }

  "It is not launched if not specified" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("local-test")
    val sparkContext = new SparkContext(conf)

    try {
      assert(!Aurora4SparkDriver.launched, "Expect the driver to have not been launched")
      assert(
        !Aurora4SparkExecutorPlugin.launched && Aurora4SparkExecutorPlugin.params.isEmpty,
        "Expect the executor plugin to have not been launched"
      )
    } finally sparkContext.stop()
  }

  "It is launched if specified" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("local-test")
    conf.set("spark.plugins", classOf[SqlPlugin].getName)
    val sparkContext = new SparkContext(conf)
    try {
      assert(Aurora4SparkDriver.launched, "Expect the driver to have been launched")
      assert(
        Aurora4SparkExecutorPlugin.launched && !Aurora4SparkExecutorPlugin.params.isEmpty,
        "Expect the executor plugin to have been launched"
      )
    } finally sparkContext.stop()
  }

  "It properly passes aruments to spark executor plugin" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("local-test")
    conf.set("spark.plugins", classOf[SqlPlugin].getName)
    val sparkContext = new SparkContext(conf)
    try {
      assert(
        Aurora4SparkExecutorPlugin.params == Map("testArgument" -> "test"),
        "Expect arguments to be passed from driver to executor plugin"
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
    info("""
           |We do this so that we know exactly what we need to optimize/rewrite for GPU processing
           |
           |We will get there more quickly by doing this sort of use case based reverse engineering.
           |""".stripMargin)
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
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
        SparkPlanSavingPlugin.savedSparkPlan.getClass.getCanonicalName ==
          "org.apache.spark.sql.execution.aggregate.HashAggregateExec"
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
            child.getClass.getCanonicalName ==
              "org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"
          )
          child match {
            case second @ org.apache.spark.sql.execution.exchange
                  .ShuffleExchangeExec(outputPartitioning, child2, shuffleOrigin) =>
              info(s"Second root of the plan: ${second}")
              assert(
                child2.getClass.getCanonicalName ==
                  "org.apache.spark.sql.execution.aggregate.HashAggregateExec"
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
                    child3.getClass.getCanonicalName ==
                      "org.apache.spark.sql.execution.LocalTableScanExec"
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

  "From the execution plan, we get the inputted numbers" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      Seq(1D, 2D, 3D)
        .toDS()
        .createOrReplaceTempView("nums")

      val result =
        sparkSession.sql("SELECT SUM(value) FROM nums").as[Double].head()

      assert(
        SparkPlanSavingPlugin.savedSparkPlan.getClass.getCanonicalName
          == "org.apache.spark.sql.execution.aggregate.HashAggregateExec"
      )

      assert(
        SumPlanExtractor
          .matchPlan(SparkPlanSavingPlugin.savedSparkPlan)
          .contains(List(1, 2, 3))
      )
    } finally sparkSession.close()
  }

  "We can do a pretend plug-in that will return 6" in {
    info("""
      This enables us to be confident this is the right place to do the rewriting.
      The significance of this is that from here,
      we will be able to compare expected results with actual results after we apply the plug-in
      """)
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[DummyShortCircuitSqlPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._

      DummyShortCircuitSqlPlugin.applyShortCircuit = false
      List[BigDecimal](1, 2, 3)
        .toDS()
        .createOrReplaceTempView("nums")
      val sumDataSet =
        sparkSession.sql("SELECT SUM(value) FROM nums").as[BigDecimal]

      DummyShortCircuitSqlPlugin.applyShortCircuit = true
      val result = sumDataSet.head()

      assert(result == BigDecimal(6))
    } finally sparkSession.close()
  }

  "We call VE over SSH using the Python script, and get the right sum back from it" taggedAs
    AcceptanceTest in {
      val conf = new SparkConf()
      conf.setMaster("local")
      conf.set("spark.ui.enabled", "false")
      conf.set("spark.sql.extensions", classOf[SummingPlugin].getCanonicalName)
      conf.setAppName("local-test")
      val sparkSession = SparkSession.builder().config(conf).getOrCreate()
      try {
        import sparkSession.implicits._
        SummingPlugin.enable = false

        val nums: List[Double] = List(1, 2, 3, 4, (Math.abs(scala.util.Random.nextInt() % 200))
        )

        info(s"Input: ${nums}")

        nums
          .toDS()
          .createOrReplaceTempView("nums")

        SummingPlugin.enable = true
        SummingPlugin.summer = BigDecimalSummer.PythonNecSSHSummer

        val sumDataSet =
          sparkSession.sql("SELECT SUM(value) FROM nums").as[Double]
        val result = sumDataSet.head()

        info(s"Result of sum = $result")
        assert(result == BigDecimalSummer.ScalaSummer.sum(nums.map(BigDecimal(_))))
      } finally sparkSession.close()
    }

  "We call VE over SSH using a Bundle, and get the right sum back from it" taggedAs
    AcceptanceTest in {
      markup("SUM() of single column")
      val conf = new SparkConf()
      conf.setMaster("local")
      conf.set("spark.ui.enabled", "false")
      conf.set("spark.sql.extensions", classOf[SummingPlugin].getCanonicalName)
      conf.setAppName("local-test")
      val sparkSession = SparkSession.builder().config(conf).getOrCreate()
      try {
        import sparkSession.implicits._
        SummingPlugin.enable = false

        val nums: List[Double] = List(1, 2, 3, 4, (Math.abs(scala.util.Random.nextInt() % 200)))
        info(s"Input: ${nums}")

        nums
          .toDS()
          .createOrReplaceTempView("nums")

        SummingPlugin.enable = true
        SummingPlugin.summer = BigDecimalSummer.BundleNecSSHSummer

        val sumDataSet =
          sparkSession.sql("SELECT SUM(value) FROM nums").as[Double]
        val result = sumDataSet.head()

        info(s"Result of sum = $result")
        assert(result == BigDecimalSummer.ScalaSummer.sum(nums.map(BigDecimal(_))).toDouble)
      } finally sparkSession.close()
    }

  "We call the Scala summer, with a CSV input" in {
    info(
      "The goal here is to verify that we can read from " +
        "different input sources for our evaluations."
    )
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SummingPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      SummingPlugin.enable = false
      SummingPlugin.summer = BigDecimalSummer.ScalaSummer

      SummingPlugin.enable = true

      val sumDataSet = sparkSession.read
        .format("csv")
        .schema(StructType(Seq(StructField("value", DoubleType, nullable = false))))
        .load(Paths.get(this.getClass.getResource("/sample.csv").toURI).toAbsolutePath.toString)
        .as[Double]
        .selectExpr("SUM(value)")
        .as[Double]

      sumDataSet.explain(true)
      val result = sumDataSet.head()

      info(s"Result of sum = $result")
      assert(result == 62D)
    } finally sparkSession.close()
  }

  import SparkPlanSavingPlugin.savedSparkPlan

  "We match the averaging plan" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      Seq[Double](1, 2, 3)
        .toDS()
        .createOrReplaceTempView("nums")

      sparkSession.sql("SELECT AVG(value) FROM nums").as[Double].head()

      info("\n" + savedSparkPlan.toString())
      assert(AveragingPlanner.matchPlan(savedSparkPlan).isDefined, savedSparkPlan.toString())
    } finally sparkSession.close()
  }

  "We match multiple average functions" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      Seq[Double](1, 2, 3)
        .toDS()
        .createOrReplaceTempView("nums")

      sparkSession.sql("SELECT AVG(value), AVG(value) FROM nums").as[(Double, Double)].head()

      info("\n" + savedSparkPlan.toString())
      assert(AveragingPlanner.matchPlan(savedSparkPlan).isDefined, savedSparkPlan.toString())
    } finally sparkSession.close()
  }

  "Summing plan does not match in the averaging plan" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      Seq[Double](1, 2, 3)
        .toDS()
        .createOrReplaceTempView("nums")

      sparkSession.sql("SELECT SUM(value) FROM nums").as[Double].head()

      assert(AveragingPlanner.matchPlan(savedSparkPlan).isEmpty, savedSparkPlan.toString())
    } finally sparkSession.close()
  }

  "Summing plan does not match the averaging plan" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      Seq[Double](1, 2, 3)
        .toDS()
        .createOrReplaceTempView("nums")

      sparkSession.sql("SELECT AVG(value) FROM nums").as[Double].head()

      assert(SumPlanExtractor.matchPlan(savedSparkPlan).isEmpty, savedSparkPlan.toString())
    } finally sparkSession.close()
  }

  "We call VE with our Averaging plan" taggedAs
    AcceptanceTest in {
      markup("AVG()")
      val conf = new SparkConf()
      conf.setMaster("local")
      conf.set("spark.ui.enabled", "false")
      conf.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      conf.setAppName("local-test")
      val sparkSession = SparkSession.builder().config(conf).getOrCreate()
      try {
        import sparkSession.implicits._

        val nums = List[Double](1, 2, 3, 4, Math.abs(scala.util.Random.nextInt() % 200))
        info(s"Input: ${nums}")

        nums
          .toDS()
          .createOrReplaceTempView("nums")

        SparkSqlPlanExtension.rulesToApply.append { (sparkPlan) =>
          AveragingPlanner
            .matchPlan(sparkPlan)
            .map { childPlan =>
              AveragingSparkPlanMultipleColumns(
                childPlan.sparkPlan,
                childPlan.attributes,
                AveragingSparkPlanMultipleColumns.averageLocalScala
              )
            }
            .getOrElse(fail("Not expected to be here"))
        }

        val sumDataSet =
          sparkSession.sql("SELECT AVG(value) FROM nums").as[Double]

        val result = sumDataSet.head()

        assert(result == nums.sum / nums.length)
      } finally sparkSession.close()
    }

  "Spark's AVG() function returns a different Scale from SUM()" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    try {
      import sparkSession.implicits._

      val nums = List.empty[BigDecimal]

      nums
        .toDS()
        .createOrReplaceTempView("nums")

      SparkSqlPlanExtension.rulesToApply.append { _ => StaticNumberPlan(5) }

      val sumQuery = sparkSession.sql("SELECT SUM(value) FROM nums").as[BigDecimal]
      val avgQuery = sparkSession.sql("SELECT AVG(value) FROM nums").as[BigDecimal]

      assert(
        sumQuery.head() == BigDecimal(5) && avgQuery.head() == BigDecimal(0.0005)
          && sumQuery.schema.head.dataType.asInstanceOf[DecimalType] == DecimalType(38, 18)
          && avgQuery.schema.head.dataType.asInstanceOf[DecimalType] == DecimalType(38, 22)
      )

    } finally sparkSession.close()
  }

  "Sum plan matches sum of two columns" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      Seq[(Double, Double)]((1, 2), (3, 4), (5, 6))
        .toDS()
        .createOrReplaceTempView("nums")

      sparkSession.sql("SELECT SUM(_1 + _2) FROM nums").as[Double].head()
      info("\n" + savedSparkPlan.toString())
      assert(SumPlanExtractor.matchPlan(savedSparkPlan).isDefined, savedSparkPlan.toString())

    } finally sparkSession.close()
  }

  "Sum plan matches sum of three columns" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      Seq[(Double, Double, Double)]((1, 2, 3), (3, 4, 4), (5, 6, 7))
        .toDS()
        .createOrReplaceTempView("nums")

      sparkSession.sql("SELECT SUM(_1 + _2 + _3) FROM nums").as[Double].head()
      info("\n" + savedSparkPlan.toString())
      assert(SumPlanExtractor.matchPlan(savedSparkPlan).isDefined, savedSparkPlan.toString())
    } finally sparkSession.close()
  }

  "Sum plan matches two sum queries" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      Seq[(Double, Double, Double)]((1, 2, 3), (3, 4, 4), (5, 6, 7))
        .toDS()
        .createOrReplaceTempView("nums")

      sparkSession.sql("SELECT SUM(_1), SUM(_1 +_2) FROM nums")
        .as[(Double, Double)].head()
      info("\n" + savedSparkPlan.toString())
      assert(SumPlanExtractor.matchPlan(savedSparkPlan).isDefined, savedSparkPlan.toString())
    } finally sparkSession.close()
  }

  "Sum plan matches three sum queries" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      Seq[(Double, Double, Double)]((1, 2, 3), (3, 4, 4), (5, 6, 7))
        .toDS()
        .createOrReplaceTempView("nums")

      sparkSession
        .sql("SELECT SUM(_1), SUM(_1 +_2), SUM(_3) FROM nums")
        .as[(Double, Double, Double)]
        .head()

      info("\n" + savedSparkPlan.toString())
      assert(SumPlanExtractor.matchPlan(savedSparkPlan).isDefined, savedSparkPlan.toString())
    } finally sparkSession.close()
  }

  "Sum plan extracts correct numbers flattened for three columns" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      Seq[(Double, Double, Double)]((1, 2, 3), (4, 5, 6), (7, 8, 9))
        .toDS()
        .createOrReplaceTempView("nums")

      sparkSession.sql("SELECT SUM(_1 + _2 + _3) FROM nums").as[Double].head()
      info("\n" + savedSparkPlan.toString())
      assert(SumPlanExtractor.matchPlan(savedSparkPlan)
        .contains(List(1D, 4D, 7D, 2D, 5D, 8D, 3D, 6D, 9D))
      )
    } finally sparkSession.close()
  }

  "We call VE over SSH using the Python script, and get the right sum back from it " +
    "in case of multiple columns sum" taggedAs
    AcceptanceTest in {
    markup("SUM() multiple columns e.g. SUM(a + b)")
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SummingPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      SummingPlugin.enable = false

      val nums: List[(Double, Double, Double)] = List(
        (1, 2, 3),
        (4, 5, 6),
        (7, 8, 9)
      )

      info(s"Input: ${nums}")

      nums
        .toDS()
        .createOrReplaceTempView("nums")

      SummingPlugin.enable = true
      SummingPlugin.summer = BigDecimalSummer.PythonNecSSHSummer

      val sumDataSet =
        sparkSession.sql("SELECT SUM(_1 + _2 + _3) FROM nums")
        .as[Double]
      val result = sumDataSet.head()

      val flattened = nums.flatMap{
        case (first, second, third) => Seq(first, second, third)
      }
      info(s"Result of sum = $result")

      assert(result == BigDecimalSummer.ScalaSummer.sum(flattened.map(BigDecimal(_))))
    } finally sparkSession.close()
  }

  "We call VE over SSH using a Bundle, and get the right sum back from it, in case of " +
    "multiple column sum" taggedAs
    AcceptanceTest in {
    markup("SUM() of single column")
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SummingPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      SummingPlugin.enable = false

      val randomNumber = Math.abs(scala.util.Random.nextInt() % 200)
      val nums: List[(Double, Double, Double)] = List(
        (1, 2, 3),
        (4, 5, 6),
        (7, 8, 9),
        (randomNumber, randomNumber, randomNumber)
      )
      info(s"Input: ${nums}")

      nums
        .toDS()
        .createOrReplaceTempView("nums")

      SummingPlugin.enable = true
      SummingPlugin.summer = BigDecimalSummer.BundleNecSSHSummer

      val sumDataSet =
        sparkSession.sql("SELECT SUM(_1 + _2 + _3) FROM nums")
          .as[Double]

      val result = sumDataSet.head()

      val flattened = nums.flatMap {
        case (first, second, third) => Seq(BigDecimal(first), BigDecimal(second), BigDecimal(third))
      }

      info(s"Result of sum = $result")
      assert(result == BigDecimalSummer.ScalaSummer.sum(flattened))
    } finally sparkSession.close()
  }

  "We call the Scala summer, with a CSV input for data with multiple columns" in {
    info(
      "The goal here is to verify that we can read from " +
        "different input sources for our evaluations."
    )
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SummingPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      SummingPlugin.enable = false
      SummingPlugin.summer = BigDecimalSummer.ScalaSummer

      SummingPlugin.enable = true
      val csvSchema = StructType(
        Seq(
          StructField("a", DoubleType, nullable = false),
          StructField("b", DoubleType, nullable = false)
        )
      )
      val sumDataSet = sparkSession.read
        .format("csv")
        .schema(csvSchema)
        .load(Paths.get(this
          .getClass
          .getResource("/sampleMultiColumn.csv")
          .toURI).toAbsolutePath.toString
        )
        .as[(Double, Double)]
        .selectExpr("SUM(a + b)")
        .as[Double]

      sumDataSet.explain(true)
      val result = sumDataSet.collect().head

      info(s"Result of sum = $result")
      assert(result == 82D)
    } finally sparkSession.close()
  }

  "We call the Scala summer, with a CSV input for multiple SUM operations" in {
    info(
      "The goal here is to verify that we can read from " +
        "different input sources for our evaluations."
    )
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SummingPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._

      SummingPlugin.enable = false
      SummingPlugin.summer = BigDecimalSummer.ScalaSummer

      SummingPlugin.enable = true
      val csvSchema = StructType(
        Seq(
          StructField("a", DoubleType, nullable = false),
          StructField("b", DoubleType, nullable = false)
        )
      )
      val sumDataSet = sparkSession.read
        .format("csv")
        .schema(csvSchema)
        .load(Paths.get(this
          .getClass
          .getResource("/sampleMultiColumn.csv")
          .toURI).toAbsolutePath.toString
        )
        .as[(Double, Double)]
        .selectExpr("SUM(a)", "SUM(b)")
        .as[(Double, Double)]

      sumDataSet.explain(true)
      val result = sumDataSet.collect().head

      info(s"Result of sum = $result")
      assert(result == (62D, 20D))
    } finally sparkSession.close()
  }

  "We call VE over SSH using a Bundle, and get the right sum back from it, in case of " +
    "multiple sum operations" taggedAs
    AcceptanceTest in {
    markup("SUM() of single column")
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SummingPlugin].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._
      SummingPlugin.enable = false

      val randomNumber = Math.abs(scala.util.Random.nextInt() % 200)
      val nums: List[(Double, Double, Double)] = List(
        (1, 2, 3),
        (4, 5, 6),
        (7, 8, 9),
        (randomNumber, randomNumber, randomNumber)
      )
      info(s"Input: ${nums}")

      nums
        .toDS()
        .createOrReplaceTempView("nums")

      SummingPlugin.enable = true
      SummingPlugin.summer = BigDecimalSummer.BundleNecSSHSummer

      val sumDataSet =
        sparkSession.sql("SELECT SUM(_1), SUM(_2), SUM(_3), SUM(_1 + _2 + _3) FROM nums")
          .as[(Double, Double, Double, Double)]

      val result = sumDataSet.head()

      val expected = (
        nums.map(_._1).sum,
        nums.map(_._2).sum,
        nums.map(_._3).sum,
        nums.flatMap(elem => Seq(elem._1, elem._2, elem._3)).sum,
      )

      info(s"Result of sum = $result")

      assert(result == expected)
    } finally sparkSession.close()
  }

  "We call VE with our Averaging plan for multiple average operations" taggedAs
    AcceptanceTest in {
    markup("AVG()")
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._

      val nums: List[(Double, Double, Double)] = List(
        (1, 2, 3),
        (4, 5, 6),
        (7, 8, 9)
      )
      info(s"Input: ${nums}")

      nums
        .toDS()
        .createOrReplaceTempView("nums")

      SparkSqlPlanExtension.rulesToApply.append { (sparkPlan) =>
        AveragingPlanner
          .matchPlan(sparkPlan)
          .map { childPlan =>
            AveragingSparkPlanMultipleColumns(childPlan.sparkPlan,
              childPlan.attributes,
              AveragingSparkPlanMultipleColumns.averageLocalScala)
          }
          .getOrElse(fail("Not expected to be here"))
      }

      val sumDataSet =
        sparkSession.sql("SELECT AVG(_1), AVG(_2), AVG(_3) FROM nums")
          .as[(Double, Double, Double)]

      val expected = (
        nums.map(_._1).sum/nums.size,
        nums.map(_._2).sum/nums.size,
        nums.map(_._3).sum/nums.size
      )

      val result = sumDataSet.collect().head

      assert(result == expected)
    } finally sparkSession.close()
  }
}
