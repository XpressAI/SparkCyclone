package com.nec.spark.agile

import java.util.Properties

import com.nec.debugging.Debugging.SprarkSessionImplicit
import com.nec.h2.H2DatabaseConnector
import com.nec.spark.{Aurora4SparkDriver, Aurora4SparkExecutorPlugin, SampleTestData, SparkAdditions}
import com.nec.spark.planning.SingleColumnSumPlanExtractor
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

import org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED

/**
 * These tests are to get familiar with Spark and encode any oddities about it.
 */
final class SparkPlansSpec
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {
  before {
    H2DatabaseConnector.init()
  }
  "We get the spark plan for more complex query" in withSparkSession(
    _.set(WHOLESTAGE_CODEGEN_ENABLED.key, "false")
  ) { sparkSession =>
    import sparkSession.implicits._
    val props = new Properties()

    sparkSession.read
      .jdbc(H2DatabaseConnector.inputH2Url, "Users", props)
      .createOrReplaceTempView("Users")

    sparkSession
      .read
      .option("header", "true")
      .csv(SampleTestData.OrdersCsv.toString)
      .createOrReplaceTempView("Orders")


    sparkSession
      .debugSql("SELECT SUM(totalPrice) as totalSum, userId, joined_timestamp FROM Users JOIN Orders on Orders.userId = Users.id " +
        "GROUP BY userId, joined_timestamp ORDER BY joined_timestamp ASC", "real-life-example")
      .show()
  }
}
