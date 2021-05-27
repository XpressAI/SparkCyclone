package acc

import com.nec.spark.agile._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

final class AuroraSqlPluginTest
  extends AnyFreeSpec
  with BeforeAndAfterAll
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {

  "We call VE over SSH using a Bundle, and get the right sum back from it" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SummingPlugin].getCanonicalName)
  ) { sparkSession =>
    markup("SUM() of single column")
    import sparkSession.implicits._
    SummingPlugin.enable = false

    val nums: List[Double] = List(1, 2, 3, 4, (Math.abs(scala.util.Random.nextInt() % 200)))
    info(s"Input: ${nums}")

    nums
      .toDS()
      .createOrReplaceTempView("nums")

    SummingPlugin.enable = true
    SummingPlugin.summer = BigDecimalSummer.ScalaSummer

    val sumDataSet =
      sparkSession.sql("SELECT SUM(value) FROM nums").as[Double]
    val result = sumDataSet.head()

    info(s"Result of sum = $result")
    assert(result == BigDecimalSummer.ScalaSummer.sum(nums.map(BigDecimal(_))).toDouble)
  }

  "We call VE with our Averaging plan" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
  ) { sparkSession =>
    markup("AVG()")
    import sparkSession.implicits._

    val nums = List[Double](1, 2, 3, 4, Math.abs(scala.util.Random.nextInt() % 200))
    info(s"Input: ${nums}")

    nums
      .toDS()
      .createOrReplaceTempView("nums")
    SparkSqlPlanExtension.rulesToApply.clear()
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
  }

  "We call VE over SSH using the Python script, and get the right sum back from it " +
    "in case of multiple columns sum" in withSparkSession(
      _.set("spark.sql.extensions", classOf[SummingPlugin].getCanonicalName)
    ) { sparkSession =>
      markup("SUM() multiple columns e.g. SUM(a + b)")
      import sparkSession.implicits._
      SummingPlugin.enable = false

      val nums: List[(Double, Double, Double)] = List((1, 2, 3), (4, 5, 6), (7, 8, 9))

      info(s"Input: ${nums}")

      nums
        .toDS()
        .createOrReplaceTempView("nums")

      SummingPlugin.enable = true
      SummingPlugin.summer = BigDecimalSummer.ScalaSummer

      val sumDataSet =
        sparkSession
          .sql("SELECT SUM(_1 + _2 + _3) FROM nums")
          .as[Double]
      val result = sumDataSet.head()

      val flattened = nums.flatMap { case (first, second, third) =>
        Seq(first, second, third)
      }
      info(s"Result of sum = $result")

      assert(result == BigDecimalSummer.ScalaSummer.sum(flattened.map(BigDecimal(_))))
    }

  "We call VE over SSH using a Bundle, and get the right sum back from it, in case of " +
    "multiple column sum" in withSparkSession(
      _.set("spark.sql.extensions", classOf[SummingPlugin].getCanonicalName)
    ) { sparkSession =>
      markup("SUM() of single column")
      import sparkSession.implicits._
      SummingPlugin.enable = false

      val randomNumber = Math.abs(scala.util.Random.nextInt() % 200)
      val nums: List[(Double, Double, Double)] =
        List((1, 2, 3), (4, 5, 6), (7, 8, 9), (randomNumber, randomNumber, randomNumber))
      info(s"Input: ${nums}")

      nums
        .toDS()
        .createOrReplaceTempView("nums")

      SummingPlugin.enable = true
      SummingPlugin.summer = BigDecimalSummer.ScalaSummer

      val sumDataSet =
        sparkSession
          .sql("SELECT SUM(_1 + _2 + _3) FROM nums")
          .as[Double]

      val result = sumDataSet.head()

      val flattened = nums.flatMap { case (first, second, third) =>
        Seq(BigDecimal(first), BigDecimal(second), BigDecimal(third))
      }

      info(s"Result of sum = $result")
      assert(result == BigDecimalSummer.ScalaSummer.sum(flattened))
    }

  "We call VE over SSH using a Bundle, and get the right sum back from it, in case of " +
    "multiple sum operations" in withSparkSession(
      _.set("spark.sql.extensions", classOf[SummingPlugin].getCanonicalName)
    ) { sparkSession =>
      markup("SUM() of single column")
      import sparkSession.implicits._
      SummingPlugin.enable = false

      val randomNumber = Math.abs(scala.util.Random.nextInt() % 200)
      val nums: List[(Double, Double, Double)] =
        List((1, 2, 3), (4, 5, 6), (7, 8, 9), (randomNumber, randomNumber, randomNumber))
      info(s"Input: ${nums}")

      nums
        .toDS()
        .createOrReplaceTempView("nums")
      SummingPlugin.enable = true
      SummingPlugin.summer = BigDecimalSummer.ScalaSummer

      val sumDataSet =
        sparkSession
          .sql("SELECT SUM(_1), SUM(_2), SUM(_3), SUM(_1 + _2 + _3) FROM nums")
          .as[(Double, Double, Double, Double)]

      val result = sumDataSet.head()

      val expected = (
        nums.map(_._1).sum,
        nums.map(_._2).sum,
        nums.map(_._3).sum,
        nums.flatMap(elem => Seq(elem._1, elem._2, elem._3)).sum
      )

      info(s"Result of sum = $result")

      assert(result == expected)
    }

  "We call VE with our Averaging plan for multiple average operations" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
  ) { sparkSession =>
    markup("AVG()")
    import sparkSession.implicits._

    val nums: List[(Double, Double, Double)] = List((1, 2, 3), (4, 5, 6), (7, 8, 9))
    info(s"Input: $nums")

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
      sparkSession
        .sql("SELECT AVG(_1), AVG(_2), AVG(_3) FROM nums")
        .as[(Double, Double, Double)]

    val expected = (
      nums.map(_._1).sum / nums.size,
      nums.map(_._2).sum / nums.size,
      nums.map(_._3).sum / nums.size
    )

    val result = sumDataSet.collect().head

    assert(result == expected)
  }

}
