package com.nec.spark.agile

import com.nec.debugging.Debugging.RichDataSet

import java.util.Properties
import com.nec.h2.H2DatabaseConnector
import com.nec.spark.SampleTestData
import com.nec.spark.SparkAdditions
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

/**
 * These tests are to get familiar with Spark and encode any oddities about it.
 */
final class RealWorldSparkPlansSpec
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {
  before {
    H2DatabaseConnector.init()
  }
  "We get the spark plan for more complex query" in withSparkSession(identity) { sparkSession =>
    import sparkSession.implicits._
    val props = new Properties()

    sparkSession.read
      .jdbc(H2DatabaseConnector.inputH2Url, "Users", props)
      .createOrReplaceTempView("Users")

    sparkSession.read
      .option("header", "true")
      .csv(SampleTestData.OrdersCsv.toString)
      .createOrReplaceTempView("Orders")

    sparkSession
      .sql(sqlText =
        "SELECT SUM(totalPrice), userId, joined_timestamp FROM Users JOIN Orders on Orders.userId = Users.id " +
          "GROUP BY userId, joined_timestamp ORDER BY joined_timestamp ASC"
      )
      .debugSqlAndShow(name = "real-life-example")
  }
}
