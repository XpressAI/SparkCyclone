package com.nec.spark.agile

import com.nec.spark.SampleTestData.SampleCSV
import com.nec.spark.agile.PairwiseAdditionOffHeap.OffHeapPairwiseSummer
import com.nec.spark.planning.SparkPlanSavingPlugin
import com.nec.spark.planning.SumPlanExtractor
import com.nec.spark.Aurora4SparkDriver
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.nec.spark.AuroraSqlPlugin
import com.nec.spark.SampleTestData.SampleMultiColumnCSV
import com.nec.spark.SampleTestData.SampleTwoColumnParquet
import com.nec.spark.SparkAdditions
import com.nec.spark.planning.AddPlanExtractor
import com.nec.spark.planning.AveragingPlanner
import com.nec.spark.planning.AveragingSparkPlanOffHeap
import com.nec.spark.planning.AveragingSparkPlanOffHeap.OffHeapDoubleAverager
import com.nec.spark.planning.GenericAggregationPlanOffHeap
import com.nec.spark.planning.MultipleColumnsAveragingPlanOffHeap.MultipleColumnsOffHeapAverager
import com.nec.spark.planning.MultipleColumnsSummingPlanOffHeap
import com.nec.spark.planning.MultipleColumnsSummingPlanOffHeap.MultipleColumnsOffHeapSummer
import com.nec.spark.planning.SparkPlanSavingPlugin.savedSparkPlan
import com.nec.spark.planning.SparkSqlPlanExtension
import com.nec.spark.planning.StaticNumberPlan
import com.nec.spark.planning.VeoGenericPlanExtractor
import com.nec.spark.planning.VeoSumPlanExtractor
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.RowToColumnarExec
import org.apache.spark.sql.internal.SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED
import org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.SparkConf
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

final class AuroraSqlPluginTest
  extends AnyFreeSpec
  with BeforeAndAfter
  with SparkAdditions
  with Matchers {

  "It is not launched if not specified" in withSpark(identity) { sparkContext =>
    assert(!Aurora4SparkDriver.launched, "Expect the driver to have not been launched")
    assert(
      !Aurora4SparkExecutorPlugin.launched && Aurora4SparkExecutorPlugin.params.isEmpty,
      "Expect the executor plugin to have not been launched"
    )
  }

  "It is launched if specified" ignore withSpark(
    _.set("spark.plugins", classOf[AuroraSqlPlugin].getName)
  ) { _ =>
    assert(Aurora4SparkDriver.launched, "Expect the driver to have been launched")
    assert(
      Aurora4SparkExecutorPlugin.launched && Aurora4SparkExecutorPlugin.params.nonEmpty,
      "Expect the executor plugin to have been launched"
    )
  }

  "It properly passes aruments to spark executor plugin" ignore withSpark(
    _.set("spark.plugins", classOf[AuroraSqlPlugin].getName)
  ) { sparkContext =>
    assert(
      Aurora4SparkExecutorPlugin.params == Map("testArgument" -> "test"),
      "Expect arguments to be passed from driver to executor plugin"
    )
  }

  "We can run a Spark-SQL job" in withSparkSession(identity) { sparkSession =>
    import sparkSession.implicits._
    val result = sparkSession.sql("SELECT 1 + 2").as[Int].collect().toList
    assert(result == List(3))
  }

  "From the execution plan, we get the inputted numbers" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq(1d, 2d, 3d)
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
  }

  "We call the Scala summer, with a CSV input" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SummingPlugin].getCanonicalName)
  ) { sparkSession =>
    info(
      "The goal here is to verify that we can read from " +
        "different input sources for our evaluations."
    )
    import sparkSession.implicits._
    SummingPlugin.enable = false
    SummingPlugin.summer = BigDecimalSummer.ScalaSummer

    SummingPlugin.enable = true

    val sumDataSet = sparkSession.read
      .format("csv")
      .schema(StructType(Seq(StructField("value", DoubleType, nullable = false))))
      .load(SampleCSV.toString)
      .as[Double]
      .selectExpr("SUM(value)")
      .as[Double]

    sumDataSet.explain(true)
    val result = sumDataSet.head()

    info(s"Result of sum = $result")
    assert(result == 62d)
  }

  "We match the averaging plan" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq[Double](1, 2, 3)
      .toDS()
      .createOrReplaceTempView("nums")

    sparkSession.sql("SELECT AVG(value) FROM nums").as[Double].head()

    info("\n" + savedSparkPlan.toString())
    assert(AveragingPlanner.matchPlan(savedSparkPlan).isDefined, savedSparkPlan.toString())
  }

  "We match multiple average functions" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq[Double](1, 2, 3)
      .toDS()
      .createOrReplaceTempView("nums")

    sparkSession.sql("SELECT AVG(value), AVG(value) FROM nums").as[(Double, Double)].head()

    info("\n" + savedSparkPlan.toString())
    assert(AveragingPlanner.matchPlan(savedSparkPlan).isDefined, savedSparkPlan.toString())
  }

  "Summing plan does not match in the averaging plan" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq[Double](1, 2, 3)
      .toDS()
      .createOrReplaceTempView("nums")

    sparkSession.sql("SELECT SUM(value) FROM nums").as[Double].head()

    assert(AveragingPlanner.matchPlan(savedSparkPlan).isEmpty, savedSparkPlan.toString())
  }

  "Summing plan does not match the averaging plan" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq[Double](1, 2, 3)
      .toDS()
      .createOrReplaceTempView("nums")

    sparkSession.sql("SELECT AVG(value) FROM nums").as[Double].head()

    assert(SumPlanExtractor.matchPlan(savedSparkPlan).isEmpty, savedSparkPlan.toString())
  }

  "We extract data with RowToColumnarExec" in withSparkSession(
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
          AveragingSparkPlanOffHeap(
            RowToColumnarExec(childPlan.sparkPlan),
            OffHeapDoubleAverager.UnsafeBased
          )
        }
        .getOrElse(fail("Not expected to be here"))
    }

    val sumDataSet =
      sparkSession.sql("SELECT AVG(value) FROM nums").as[Double]

    sumDataSet.explain(true)

    val listOfDoubles = sumDataSet.collect().toList
    info(listOfDoubles.toString())

    val result = listOfDoubles.head
    assert(result == nums.sum / nums.length)
  }

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
          GenericAggregationPlanOffHeap(
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

    sumDataSet.explain(true)

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
          GenericAggregationPlanOffHeap(
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
        .sql("SELECT AVG(nums._1 + nums._2 + nums._3) FROM nums")
        .as[Double]

    sumDataSet.explain(true)

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
          GenericAggregationPlanOffHeap(
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

    sumDataSet.explain(true)

    val listOfDoubles = sumDataSet.collect().head
    listOfDoubles shouldEqual (4.0 +- (0.00000001))
  }

  "We can sum multiple columns reading from Parquet" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
      .set(WHOLESTAGE_CODEGEN_ENABLED.key, "false")
  ) { sparkSession =>
    import sparkSession.implicits._

    SparkSqlPlanExtension.rulesToApply.clear()

    SparkSqlPlanExtension.rulesToApply.append { sparkPlan =>
      VeoSumPlanExtractor
        .matchPlan(sparkPlan)
        .map { childPlan =>
          MultipleColumnsSummingPlanOffHeap(
            RowToColumnarExec(childPlan.sparkPlan),
            MultipleColumnsSummingPlanOffHeap.MultipleColumnsOffHeapSummer.UnsafeBased,
            childPlan.attributes
          )
        }
        .getOrElse(fail(s"Not expected to be here: ${sparkPlan}"))
    }

    val sumDataSet = sparkSession.read
      .format("parquet")
      .load(SampleTwoColumnParquet.toString)
      .as[(Double, Double)]
      .selectExpr("SUM(a)")
      .as[Double]

    sumDataSet.explain(true)

    assert(sumDataSet.collect().toList == List(62))
  }

  "We Pairwise-add off the heap" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
  ) { sparkSession =>
    import sparkSession.implicits._

    SparkSqlPlanExtension.rulesToApply.clear()

    SparkSqlPlanExtension.rulesToApply.append { sparkPlan =>
      AddPlanExtractor
        .matchAddPairwisePlan(sparkPlan, OffHeapPairwiseSummer.UnsafeBased)
        .getOrElse(sys.error(s"Plan was not matched: ${sparkPlan}"))
    }

    val csvSchema = StructType(
      Seq(
        StructField("a", DoubleType, nullable = false),
        StructField("b", DoubleType, nullable = false)
      )
    )
    val sumDataSet2 = sparkSession.read
      .format("csv")
      .schema(csvSchema)
      .load(SampleMultiColumnCSV.toString)
      .as[(Double, Double)]
      .selectExpr("a + b")
      .as[Double]

    sumDataSet2.explain(true)

    val listOfDoubles = sumDataSet2.collect().toList
    assert(listOfDoubles == List(3, 5, 7, 9, 58))
  }
  "We Pairwise-add Parquet off the heap" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
      .set(COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
  ) { sparkSession =>
    import sparkSession.implicits._

    SparkSqlPlanExtension.rulesToApply.clear()

    SparkSqlPlanExtension.rulesToApply.append { sparkPlan =>
      AddPlanExtractor
        .matchAddPairwisePlan(sparkPlan, OffHeapPairwiseSummer.UnsafeBased)
        .getOrElse(sys.error(s"Plan was not matched: ${sparkPlan}"))
    }

    val sumDataSet2 = sparkSession.read
      .format("parquet")
      .load(SampleTwoColumnParquet.toString)
      .as[(Double, Double)]
      .selectExpr("a + b")
      .as[Double]

    sumDataSet2.explain(true)

    val listOfDoubles = sumDataSet2.collect().toList
    assert(listOfDoubles == List(3, 5, 7, 9, 58))
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
          GenericAggregationPlanOffHeap(
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
      sparkSession.sql("SELECT SUM(nums._1 - nums._2 - nums._3) FROM nums").as[Double]
    val secondSumWithSubtraction =
      sparkSession.sql("SELECT SUM(nums._2 - nums._3 - nums._1) FROM nums").as[Double]
    val thirdSumWithSubtraction =
      sparkSession.sql("SELECT SUM(nums._3 - nums._1 - nums._2) FROM nums").as[Double]

    firstSumWithSubtraction.explain(true)
    secondSumWithSubtraction.explain(true)
    thirdSumWithSubtraction.explain(true)

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
            GenericAggregationPlanOffHeap(
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

      sumDataSet.explain(true)

      val listOfDoubles = sumDataSet.collect().head
      assert(listOfDoubles == (15.0, 28.0, 40.0))
    } finally sparkSession.close()
  }

  "We can peform multiple different operations separately off the heap" in {
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
            GenericAggregationPlanOffHeap(
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

      sumDataSet.explain(true)

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
            GenericAggregationPlanOffHeap(
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

      sumDataSet.explain(true)

      val listOfDoubles = sumDataSet.collect().head
      assert(listOfDoubles == (45.0, 5.0, 40.0))
    } finally sparkSession.close()
  }

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
          GenericAggregationPlanOffHeap(
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
      sparkSession.sql("SELECT SUM(nums._1 + nums._2) FROM nums").as[Double]

    sumDataSet.explain(true)

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
          GenericAggregationPlanOffHeap(
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
      sparkSession.sql("SELECT SUM(nums._1 - nums._2) FROM nums").as[Double]

    sumDataSet.explain(true)

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
          GenericAggregationPlanOffHeap(
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
      sparkSession.sql("SELECT SUM(nums._1) FROM nums").as[Double]

    sumDataSet.explain(true)

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
          GenericAggregationPlanOffHeap(
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
      sparkSession.sql("SELECT SUM(value) FROM nums").as[Double]

    sumDataSet.explain(true)

    val listOfDoubles = sumDataSet.collect().toList
    info(listOfDoubles.toString())

    val result = listOfDoubles.head
    assert(result == nums.sum)
  }

  "Spark's AVG() function returns a different Scale from SUM()" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkSqlPlanExtension].getCanonicalName)
  ) { sparkSession =>
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
  }

  "Sum plan matches sum of two columns" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq[(Double, Double)]((1, 2), (3, 4), (5, 6))
      .toDS()
      .createOrReplaceTempView("nums")

    sparkSession.sql("SELECT SUM(_1 + _2) FROM nums").as[Double].head()
    info("\n" + savedSparkPlan.toString())
    assert(SumPlanExtractor.matchPlan(savedSparkPlan).isDefined, savedSparkPlan.toString())
  }

  "Sum plan matches sum of three columns" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq[(Double, Double, Double)]((1, 2, 3), (3, 4, 4), (5, 6, 7))
      .toDS()
      .createOrReplaceTempView("nums")

    sparkSession.sql("SELECT SUM(_1 + _2 + _3) FROM nums").as[Double].head()
    info("\n" + savedSparkPlan.toString())
    assert(SumPlanExtractor.matchPlan(savedSparkPlan).isDefined, savedSparkPlan.toString())
  }

  "Sum plan matches two sum queries" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq[(Double, Double, Double)]((1, 2, 3), (3, 4, 4), (5, 6, 7))
      .toDS()
      .createOrReplaceTempView("nums")

    sparkSession
      .sql("SELECT SUM(_1), SUM(_1 +_2) FROM nums")
      .as[(Double, Double)]
      .head()
    info("\n" + savedSparkPlan.toString())
    assert(SumPlanExtractor.matchPlan(savedSparkPlan).isDefined, savedSparkPlan.toString())
  }

  "Sum plan matches three sum queries" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
  ) { sparkSession =>
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
  }

  "Sum plan extracts correct numbers flattened for three columns" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SparkPlanSavingPlugin].getCanonicalName)
  ) { sparkSession =>
    import sparkSession.implicits._
    Seq[(Double, Double, Double)]((1, 2, 3), (4, 5, 6), (7, 8, 9))
      .toDS()
      .createOrReplaceTempView("nums")

    sparkSession.sql("SELECT SUM(_1 + _2 + _3) FROM nums").as[Double].head()
    info("\n" + savedSparkPlan.toString())
    assert(
      SumPlanExtractor
        .matchPlan(savedSparkPlan)
        .contains(List(1d, 4d, 7d, 2d, 5d, 8d, 3d, 6d, 9d))
    )
  }

  "We call the Scala summer, with a CSV input for data with multiple columns" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SummingPlugin].getCanonicalName)
  ) { sparkSession =>
    info(
      "The goal here is to verify that we can read from " +
        "different input sources for our evaluations."
    )
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
      .load(SampleMultiColumnCSV.toString)
      .as[(Double, Double)]
      .selectExpr("SUM(a + b)")
      .as[Double]

    sumDataSet.explain(true)
    val result = sumDataSet.collect().head

    info(s"Result of sum = $result")
    assert(result == 82d)
  }

  "We call the Scala summer, with a CSV input for multiple SUM operations" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SummingPlugin].getCanonicalName)
  ) { sparkSession =>
    info(
      "The goal here is to verify that we can read from " +
        "different input sources for our evaluations."
    )
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
      .load(SampleMultiColumnCSV.toString)
      .as[(Double, Double)]
      .selectExpr("SUM(a)", "SUM(b)")
      .as[(Double, Double)]

    sumDataSet.explain(true)
    val result = sumDataSet.collect().head

    info(s"Result of sum = $result")
    assert(result == (62d, 20d))
  }

  "We call the Scala summer, with a Parquet input for multiple SUM operations" in withSparkSession(
    _.set("spark.sql.extensions", classOf[SummingPlugin].getCanonicalName)
  ) { sparkSession =>
    info(
      "The goal here is to verify that we can read from " +
        "different input sources for our evaluations."
    )
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
      .load(SampleMultiColumnCSV.toString)
      .as[(Double, Double)]
      .selectExpr("SUM(a)", "SUM(b)")
      .as[(Double, Double)]

    sumDataSet.explain(true)
    val result = sumDataSet.collect().head

    info(s"Result of sum = $result")
    assert(result == (62d, 20d))
  }

  private def createUnsafeAggregator(aggregationFunction: AggregationFunction): Aggregator = {
    aggregationFunction match {
      case SumAggregation => new SumAggregator(MultipleColumnsOffHeapSummer.UnsafeBased)
      case AvgAggregation => new AvgAggregator(MultipleColumnsOffHeapAverager.UnsafeBased)
    }
  }

  private def createUnsafeColumnAggregator(
    aggregationFunction: AggregationExpression
  ): ColumnAggregator = {
    aggregationFunction match {
      case SumExpression      => AdditionAggregator(MultipleColumnsOffHeapSummer.UnsafeBased)
      case SubtractExpression => SubtractionAggregator(MultipleColumnsOffHeapSubtractor.UnsafeBased)
      case _                  => NoAggregationAggregator
    }
  }
}
