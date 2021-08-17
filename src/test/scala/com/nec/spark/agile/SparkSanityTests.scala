package com.nec.spark.agile

import com.nec.spark.Aurora4SparkDriverPlugin
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.nec.spark.SparkAdditions
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

/**
 * These tests are to get familiar with Spark and encode any oddities about it.
 */
final class SparkSanityTests
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {

  "It is not launched by default" in withSpark(identity) { _ =>
    assert(!Aurora4SparkDriverPlugin.launched, "Expect the driver to have not been launched")
    assert(
      !Aurora4SparkExecutorPlugin.launched && Aurora4SparkExecutorPlugin.params.isEmpty,
      "Expect the executor plugin to have not been launched"
    )
  }

  "We can run a Spark-SQL job for a sanity test" in withSparkSession(identity) { sparkSession =>
    import sparkSession.implicits._
    val result = sparkSession.sql("SELECT 1 + 2").as[Int].collect().toList
    assert(result == List(3))
  }

}
