package com.nec.cmake

import com.nec.spark.SparkAdditions
import com.nec.spark.agile.OutputColumn
import com.nec.spark.agile.OutputColumnPlanDescription
import com.nec.spark.planning.ArrowGenericAggregationPlanOffHeap
import com.nec.spark.planning.SparkSqlPlanExtension
import com.nec.spark.planning.VeoGenericPlanExtractor
import org.scalatest.freespec.AnyFreeSpec
import org.apache.spark.sql.execution.RowToColumnarExec
import org.apache.spark.sql.internal.SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

final class AveragingSparkPlanSpec
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {

  "We can average multiple columns separately off the heap" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
  ) { sparkSession =>
    import sparkSession.implicits._

    val nums = List[(Double, Double, Double)]((1, 2, 3), (1, 5, 3), (10, 20, 30))

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
        .sql("SELECT AVG(nums._1), AVG(nums._2), AVG(nums._3) FROM nums")
        .as[(Double, Double, Double)]
        .debugConditionally()

    val listOfDoubles = sumDataSet.collect().head
    assert(listOfDoubles == (4.0, 9.0, 12.0))
  }

  "We can average added columns" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
  ) { sparkSession =>
    import sparkSession.implicits._

    val nums = List[(Double, Double, Double)]((1, 2, 3), (1, 5, 3), (10, 20, 30))

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
      sparkSession
        .sql("SELECT AVG(nums._1 + nums._2 + nums._3) FROM nums")
        .as[Double]
        .debugConditionally()

    val listOfDoubles = sumDataSet.collect().head
    assert(listOfDoubles == 25.0)
  }

  "We can average subtracted columns" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
  ) { sparkSession =>
    import sparkSession.implicits._

    val nums = List[(Double, Double, Double)]((1, 2, 6), (1, 4, 11), (10, 20, 33))

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
        .sql("SELECT AVG(nums._3 - nums._2 - nums._1) FROM nums")
        .as[Double]
        .debugConditionally()

    val listOfDoubles = sumDataSet.collect().head
    listOfDoubles shouldEqual (4.0 +- (0.00000001))
  }

}
