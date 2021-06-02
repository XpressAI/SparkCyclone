package com.nec.spark.planning

import com.nec.spark.SparkAdditions
import com.nec.spark.agile.OutputColumn
import com.nec.spark.agile.OutputColumnPlanDescription
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.freespec.AnyFreeSpec
import org.apache.spark.sql.execution.RowToColumnarExec
import org.apache.spark.sql.internal.SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.must.Matchers

final class VeoGenericPlanExtractorSpec
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {

  "We can sum multiple columns off the heap" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
  ) { sparkSession =>
    import sparkSession.implicits._

    val nums = List[(Double, Double)]((1, 2), (4, 6), (10, 20))

    SparkSqlPlanExtension.rulesToApply.clear()

    nums
      .toDS()
      .createOrReplaceTempView("nums")

    SparkSqlPlanExtension.rulesToApply.append { sparkPlan =>
      VeoGenericPlanExtractor
        .matchPlan(sparkPlan)
        .map { childPlan =>
          ArrowGenericAggregationPlanOffHeap(
            RowToColumnarExec(childPlan.sparkPlan),
            childPlan.outColumns.map {
              case OutputColumnPlanDescription(
                    inputColumns,
                    outputColumnIndex,
                    columnAggregation,
                    outputAggregator
                  ) =>
                OutputColumn(
                  inputColumns,
                  outputColumnIndex,
                  createUnsafeColumnAggregator(columnAggregation),
                  createUnsafeAggregator(outputAggregator)
                )
            }
          )
        }
        .getOrElse(fail("Not expected to be here"))
    }

    val sumDataSet =
      sparkSession.sql("SELECT SUM(nums._1 + nums._2) FROM nums").as[Double].debugConditionally()

    val listOfDoubles = sumDataSet.collect().head
    assert(listOfDoubles == 43.0)
  }

  "We can sum subtracted columns off the heap" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
  ) { sparkSession =>
    import sparkSession.implicits._

    val nums = List[(Double, Double, Double)]((1, 2, 3), (4, 6, 7), (10, 20, 30))

    SparkSqlPlanExtension.rulesToApply.clear()

    nums
      .toDS()
      .createOrReplaceTempView("nums")

    SparkSqlPlanExtension.rulesToApply.append { sparkPlan =>
      VeoGenericPlanExtractor
        .matchPlan(sparkPlan)
        .map { childPlan =>
          ArrowGenericAggregationPlanOffHeap(
            childPlan.sparkPlan,
            childPlan.outColumns.map {
              case OutputColumnPlanDescription(
                    inputColumns,
                    outputColumnIndex,
                    columnAggregation,
                    outputAggregator
                  ) =>
                OutputColumn(
                  inputColumns,
                  outputColumnIndex,
                  createUnsafeColumnAggregator(columnAggregation),
                  createUnsafeAggregator(outputAggregator)
                )
            }
          )
        }
        .getOrElse(fail("Not expected to be here"))
    }

    val sumDataSet =
      sparkSession.sql("SELECT SUM(nums._1 - nums._2) FROM nums").as[Double].debugConditionally()

    val listOfDoubles = sumDataSet.collect().head
    assert(listOfDoubles == -13.0)
  }

  "We can sum a single column off the heap" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
  ) { sparkSession =>
    import sparkSession.implicits._

    val nums = List[(Double, Double)]((1, 2), (4, 6), (10, 20))

    SparkSqlPlanExtension.rulesToApply.clear()

    nums
      .toDS()
      .createOrReplaceTempView("nums")

    SparkSqlPlanExtension.rulesToApply.append { sparkPlan =>
      VeoGenericPlanExtractor
        .matchPlan(sparkPlan)
        .map { childPlan =>
          ArrowGenericAggregationPlanOffHeap(
            RowToColumnarExec(childPlan.sparkPlan),
            childPlan.outColumns.map {
              case OutputColumnPlanDescription(
                    inputColumns,
                    outputColumnIndex,
                    columnAggregation,
                    outputAggregator
                  ) =>
                OutputColumn(
                  inputColumns,
                  outputColumnIndex,
                  createUnsafeColumnAggregator(columnAggregation),
                  createUnsafeAggregator(outputAggregator)
                )
            }
          )
        }
        .getOrElse(fail("Not expected to be here"))
    }

    val sumDataSet =
      sparkSession.sql("SELECT SUM(nums._1) FROM nums").as[Double].debugConditionally()

    val listOfDoubles = sumDataSet.collect().head
    assert(listOfDoubles == 15.0)
  }

  "We can sum off heap" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
  ) { sparkSession =>
    import sparkSession.implicits._

    val nums = List[Double](1, 2, 3, 4, Math.abs(scala.util.Random.nextInt() % 200))

    SparkSqlPlanExtension.rulesToApply.clear()

    nums
      .toDS()
      .createOrReplaceTempView("nums")

    SparkSqlPlanExtension.rulesToApply.append { sparkPlan =>
      VeoGenericPlanExtractor
        .matchPlan(sparkPlan)
        .map { childPlan =>
          ArrowGenericAggregationPlanOffHeap(
            RowToColumnarExec(childPlan.sparkPlan),
            childPlan.outColumns.map {
              case OutputColumnPlanDescription(
                    inputColumns,
                    outputColumnIndex,
                    columnAggregation,
                    outputAggregator
                  ) =>
                OutputColumn(
                  inputColumns,
                  outputColumnIndex,
                  createUnsafeColumnAggregator(columnAggregation),
                  createUnsafeAggregator(outputAggregator)
                )
            }
          )
        }
        .getOrElse(fail("Not expected to be here"))
    }

    val sumDataSet =
      sparkSession.sql("SELECT SUM(value) FROM nums").as[Double].debugConditionally()

    val listOfDoubles = sumDataSet.collect().toList
    info(listOfDoubles.toString())

    val result = listOfDoubles.head
    assert(result == nums.sum)
  }

  "Subtracting columns keeps the order of operations" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
  ) { sparkSession =>
    import sparkSession.implicits._

    val nums = List[(Double, Double, Double)]((1, 2, 3), (4, 6, 7), (10, 20, 30))

    SparkSqlPlanExtension.rulesToApply.clear()

    nums
      .toDS()
      .createOrReplaceTempView("nums")

    SparkSqlPlanExtension.rulesToApply.append { sparkPlan =>
      VeoGenericPlanExtractor
        .matchPlan(sparkPlan)
        .map { childPlan =>
          ArrowGenericAggregationPlanOffHeap(
            RowToColumnarExec(childPlan.sparkPlan),
            childPlan.outColumns.map {
              case OutputColumnPlanDescription(
                    inputColumns,
                    outputColumnIndex,
                    columnAggregation,
                    outputAggregator
                  ) =>
                OutputColumn(
                  inputColumns,
                  outputColumnIndex,
                  createUnsafeColumnAggregator(columnAggregation),
                  createUnsafeAggregator(outputAggregator)
                )
            }
          )
        }
        .getOrElse(fail("Not expected to be here"))
    }

    val firstSumWithSubtraction =
      sparkSession
        .sql("SELECT SUM(nums._1 - nums._2 - nums._3) FROM nums")
        .as[Double]
        .debugConditionally()
    val secondSumWithSubtraction =
      sparkSession
        .sql("SELECT SUM(nums._2 - nums._3 - nums._1) FROM nums")
        .as[Double]
        .debugConditionally()
    val thirdSumWithSubtraction =
      sparkSession
        .sql("SELECT SUM(nums._3 - nums._1 - nums._2) FROM nums")
        .as[Double]
        .debugConditionally()

    val results = Seq(
      firstSumWithSubtraction.collect().head,
      secondSumWithSubtraction.collect().head,
      thirdSumWithSubtraction.collect().head
    )

    assert(results == Seq(-53.0, -27.0, -3.0))
  }

  "We can sum multiple columns separately off the heap" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
    conf.set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._

      val nums = List[(Double, Double, Double)]((1, 2, 3), (4, 6, 7), (10, 20, 30))

      SparkSqlPlanExtension.rulesToApply.clear()

      nums
        .toDS()
        .createOrReplaceTempView("nums")

      SparkSqlPlanExtension.rulesToApply.append { sparkPlan =>
        VeoGenericPlanExtractor
          .matchPlan(sparkPlan)
          .map { childPlan =>
            ArrowGenericAggregationPlanOffHeap(
              RowToColumnarExec(childPlan.sparkPlan),
              childPlan.outColumns.map {
                case OutputColumnPlanDescription(
                      inputColumns,
                      outputColumnIndex,
                      columnAggregation,
                      outputAggregator
                    ) =>
                  OutputColumn(
                    inputColumns,
                    outputColumnIndex,
                    createUnsafeColumnAggregator(columnAggregation),
                    createUnsafeAggregator(outputAggregator)
                  )
              }
            )
          }
          .getOrElse(fail("Not expected to be here"))
      }

      val sumDataSet =
        sparkSession
          .sql("SELECT SUM(nums._1), SUM(nums._2), SUM(nums._3) FROM nums")
          .as[(Double, Double, Double)]
          .debugConditionally()

      val listOfDoubles = sumDataSet.collect().head
      assert(listOfDoubles == (15.0, 28.0, 40.0))
    } finally sparkSession.close()
  }

  "We can perform multiple different operations separately off the heap" in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
    conf.set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._

      val nums = List[(Double, Double, Double)]((1, 2, 3), (4, 8, 7), (10, 20, 30))

      SparkSqlPlanExtension.rulesToApply.clear()

      nums
        .toDS()
        .createOrReplaceTempView("nums")

      SparkSqlPlanExtension.rulesToApply.append { sparkPlan =>
        VeoGenericPlanExtractor
          .matchPlan(sparkPlan)
          .map { childPlan =>
            ArrowGenericAggregationPlanOffHeap(
              RowToColumnarExec(childPlan.sparkPlan),
              childPlan.outColumns.map {
                case OutputColumnPlanDescription(
                      inputColumns,
                      outputColumnIndex,
                      columnAggregation,
                      outputAggregator
                    ) =>
                  OutputColumn(
                    inputColumns,
                    outputColumnIndex,
                    createUnsafeColumnAggregator(columnAggregation),
                    createUnsafeAggregator(outputAggregator)
                  )
              }
            )
          }
          .getOrElse(fail("Not expected to be here"))
      }

      val sumDataSet =
        sparkSession
          .sql("SELECT SUM(nums._1), AVG(nums._2), SUM(nums._3) FROM nums")
          .as[(Double, Double, Double)]
          .debugConditionally()

      val listOfDoubles = sumDataSet.collect().head
      assert(listOfDoubles == (15.0, 10.0, 40.0))
    } finally sparkSession.close()
  }

  "We can peform multiple different operations on multiple columns separately off the heap " in {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
    conf.set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
    conf.setAppName("local-test")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    try {
      import sparkSession.implicits._

      val nums = List[(Double, Double, Double)]((1, 2, 3), (4, 8, 7), (10, 20, 30))

      SparkSqlPlanExtension.rulesToApply.clear()

      nums
        .toDS()
        .createOrReplaceTempView("nums")

      SparkSqlPlanExtension.rulesToApply.append { sparkPlan =>
        VeoGenericPlanExtractor
          .matchPlan(sparkPlan)
          .map { childPlan =>
            ArrowGenericAggregationPlanOffHeap(
              RowToColumnarExec(childPlan.sparkPlan),
              childPlan.outColumns.map {
                case OutputColumnPlanDescription(
                      inputColumns,
                      outputColumnIndex,
                      columnAggregation,
                      outputAggregator
                    ) =>
                  OutputColumn(
                    inputColumns,
                    outputColumnIndex,
                    createUnsafeColumnAggregator(columnAggregation),
                    createUnsafeAggregator(outputAggregator)
                  )
              }
            )
          }
          .getOrElse(fail("Not expected to be here"))
      }

      val sumDataSet =
        sparkSession
          .sql("SELECT SUM(nums._1 + nums._2), AVG(nums._2 - nums._1), SUM(nums._3) FROM nums")
          .as[(Double, Double, Double)]
          .debugConditionally()

      val listOfDoubles = sumDataSet.collect().head
      assert(listOfDoubles == (45.0, 5.0, 40.0))
    } finally sparkSession.close()
  }

}
