package com.nec.spark.agile

import com.nec.spark.SampleTestData.SampleCSV
import com.nec.spark.SampleTestData.SampleTwoColumnParquet
import com.nec.spark.SparkAdditions
import com.nec.spark.agile.LogicalSummingPlanSpec.IdentityPlan
import com.nec.spark.agile.LogicalSummingPlanSpec.makeCsvNums
import com.nec.spark.agile.LogicalSummingPlanSpec.makeMemoryJoinNums
import com.nec.spark.agile.LogicalSummingPlanSpec.makeMemoryNums
import com.nec.spark.agile.LogicalSummingPlanSpec.makeParquetNums
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.ColumnarRule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.execution.joins.OurHashJoinExec
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec

object LogicalSummingPlanSpec {
  case class IdentityPlan(child: SparkPlan) extends SparkPlan {
    override protected def doExecute(): RDD[InternalRow] = child.execute()
    override def output: Seq[Attribute] = child.output
    override def children: Seq[SparkPlan] = Seq(child)
  }

  val SharedName = "nums"
  val SharedNameJoinView = "nums_to_join"

  def makeMemoryNums(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    Seq(1d, 2d, 3d, 4d, 52d)
      .toDS()
      .createOrReplaceTempView(SharedName)
  }

  final case class SomeTab(num: Double, mapTo: Double)
  def makeMemoryJoinNums(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    Seq(SomeTab(2d, 2.5d), SomeTab(4d, 3d))
      .toDS()
      .createOrReplaceTempView(SharedNameJoinView)
  }

  def makeCsvNums(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val schema = StructType(Array(StructField("a", DoubleType)))
    sparkSession.read
      .format("csv")
      .schema(schema)
      .load(SampleCSV.toString)
      .withColumnRenamed("a", "value")
      .as[Double]
      .createOrReplaceTempView(SharedName)
  }

  def makeParquetNums(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    sparkSession.read
      .format("parquet")
      .load(SampleTwoColumnParquet.toString)
      .withColumnRenamed("a", "value")
      .as[(Double, Double)]
      .createOrReplaceTempView(SharedName)
  }

}

//noinspection ConvertExpressionToSAM
final class LogicalSummingPlanSpec extends AnyFreeSpec with BeforeAndAfter with SparkAdditions {

  private implicit val encDouble: Encoder[Double] = Encoders.scalaDouble
  private implicit val encDouble2 =
    Encoders.tuple[Double, Double](Encoders.scalaDouble, Encoders.scalaDouble)

  "We can do an identity map" ignore {
    withVariousInputs[Double](
      _.config(CODEGEN_FALLBACK.key, value = false)
        .config("spark.sql.codegen.comments", value = true)
        .withExtensions(sse =>
          sse.injectPlannerStrategy(sparkSession =>
            new Strategy {
              override def apply(plan: LogicalPlan): Seq[SparkPlan] =
                plan match {
                  case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
                    Nil //   List(IdentityPlan(planLater(child)))
                  case _ => Nil
                }
            }
          )
        )
    )("SELECT SUM(value) FROM nums")(result => assert(result == List(62d)))
  }

  "We can do an identity codegen" - {
    withVariousInputs[Double](
      _.config(CODEGEN_FALLBACK.key, value = false)
        .config("spark.sql.codegen.comments", value = true)
        .withExtensions(sse =>
          sse.injectPlannerStrategy(sparkSession =>
            new Strategy {
              override def apply(plan: LogicalPlan): Seq[SparkPlan] =
                plan match {
                  case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
                    List(IdentityCodegenPlan(planLater(child)))
                  case _ => Nil
                }
            }
          )
        )
    )("SELECT SUM(value) FROM nums")(result => assert(result == List[Double](1, 2, 3, 4, 52)))
  }

  "We can do an identity codegen which pre-loads everything first before enabling consumption" - {

    /** To achieve this, we need to first replicate how HashAggregateExec works, as that particular plan is one that loads everything into memory first, before emitting results */
    withVariousInputs[Double](
      _.config(CODEGEN_FALLBACK.key, value = false)
        .config("spark.sql.codegen.comments", value = true)
        .withExtensions(sse =>
          sse.injectPlannerStrategy(sparkSession =>
            new Strategy {
              override def apply(plan: LogicalPlan): Seq[SparkPlan] =
                plan match {
                  case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
                    List(IdentityCopyCodegenPlan(planLater(child)))
                  case _ => Nil
                }
            }
          )
        )
    )("SELECT SUM(value) FROM nums")(result => assert(result == List[Double](1, 2, 3, 4, 52)))
  }

  "We can do a simple join" ignore {
    withVariousInputs[(Double, Double)](
      _.config(CODEGEN_FALLBACK.key, value = false)
        .config("spark.sql.codegen.comments", value = true)
        .withExtensions(sse =>
          sse.injectPlannerStrategy(sparkSession =>
            new Strategy {
              override def apply(plan: LogicalPlan): Seq[SparkPlan] =
                plan match {
                  case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
                    List(IdentityPlan(planLater(child)))
                  case _ => Nil
                }
            }
          )
        )
    )("SELECT value, mapTo FROM nums INNER JOIN nums_to_join on value = num")(result =>
      assert(result == List((2d, 2.5d), (4d, 3d)))
    )
  }

  "We can do a rewritten join" ignore {
    withVariousInputs[(Double, Double)](
      _.config(CODEGEN_FALLBACK.key, value = false)
        .config("spark.sql.codegen.comments", value = true)
        .withExtensions(sse =>
          sse.injectColumnar(sparkSession =>
            new ColumnarRule {
              override def preColumnarTransitions: Rule[SparkPlan] = { sp =>
                sp.transformDown {
                  case BroadcastHashJoinExec(
                        leftKeys,
                        rightKeys,
                        joinType,
                        buildSide,
                        condition,
                        left,
                        right,
                        isNullAwareAntiJoin
                      ) =>
                    OurHashJoinExec(
                      leftKeys,
                      rightKeys,
                      joinType,
                      buildSide,
                      condition,
                      left,
                      right,
                      isNullAwareAntiJoin
                    )
                  case other => other
                }
              }
            }
          )
        )
    )("SELECT value, mapTo + 1 FROM nums INNER JOIN nums_to_join on value + 2 = num + 3")(result =>
      assert(result == List((3d, 3.5d)))
    )
  }

  implicit class RichDataSet[T](val dataSet: Dataset[T]) {
    def debugSqlHere: Dataset[T] = {
      info(dataSet.queryExecution.executedPlan.toString())
      info(dataSet.queryExecution.debug.codegenToSeq().mkString("\n"))
      dataSet
    }
  }

  def withVariousInputs[T](
    configuration: SparkSession.Builder => SparkSession.Builder
  )(sql: String)(f: List[T] => Unit)(implicit enc: Encoder[T]): Unit = {
    for {
      (title, fr) <- List(
        "Memory" -> makeMemoryNums _,
        "CSV" -> makeCsvNums _,
        "Parquet" -> makeParquetNums _
      )
    } s"In ${title}" in withSparkSession2(configuration) { sparkSession =>
      import sparkSession.implicits._
      makeMemoryJoinNums(sparkSession)
      fr(sparkSession)
      val ds = sparkSession.sql(sql).debugSqlHere.as[T]
      f(ds.collect().toList)
    }
  }

}
