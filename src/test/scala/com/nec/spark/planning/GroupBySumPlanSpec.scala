package com.nec.spark.planning

import com.eed3si9n.expecty.Expecty.assert
import com.nec.arrow.functions.GroupBySum
import com.nec.arrow.{ArrowVectorBuilders, VeArrowNativeInterfaceNumeric}
import com.nec.spark.BenchTestingPossibilities.BenchTestAdditions
import com.nec.spark.planning.GroupBySumPlanSpec.SimpleGroupBySum.GroupByMethod
import com.nec.spark.planning.GroupBySumPlanSpec.SimpleGroupBySum.GroupByMethod.{JvmArrowBased, VEBased}
import com.nec.spark.{Aurora4SparkExecutorPlugin, AuroraSqlPlugin}
import com.nec.testing.Testing
import com.nec.testing.Testing.TestingTarget
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Dataset, SparkSession, Strategy}

object GroupBySumPlanSpec {
  object SimpleGroupBySum {
    sealed trait GroupByMethod extends Serializable
    object GroupByMethod {
        case object JvmArrowBased extends GroupByMethod
        case object VEBased extends GroupByMethod
      }
    }
  final case class SimpleGroupBySum(child: SparkPlan, groupMethod: GroupByMethod)
    extends UnaryExecNode {
    override protected def doExecute(): RDD[InternalRow] = {
      child
        .execute()
        .coalesce(1)
        .mapPartitions(iterator => {
          val inputCols = iterator.map(row => (row.getDouble(0), row.getDouble(1))).toList
          val alloc = new RootAllocator(Integer.MAX_VALUE)
          val outValuesVector = new Float8Vector("values", alloc)
          val outGroupsVector = new Float8Vector("groups", alloc)
          val resultsMap = ArrowVectorBuilders.withDirectFloat8Vector(inputCols.map(_._1)) { groupingVec =>
            ArrowVectorBuilders.withDirectFloat8Vector(inputCols.map(_._2)) { valuesVec => {
              groupMethod match {
                case JvmArrowBased =>
                  GroupBySum.groupBySumJVM(groupingVec, valuesVec)
                case VEBased =>
                  GroupBySum.runOn(
                    new VeArrowNativeInterfaceNumeric(
                      Aurora4SparkExecutorPlugin._veo_proc,
                      Aurora4SparkExecutorPlugin._veo_ctx,
                      Aurora4SparkExecutorPlugin.lib
                    ))(groupingVec, valuesVec, outGroupsVector, outValuesVector)
                  (0 until outGroupsVector.getValueCount)
                    .map(idx => (outGroupsVector.get(idx), outValuesVector.get(idx)))
                    .toMap
              }
            }}
          }

          resultsMap.zipWithIndex.map {
          val resultMap = groupMethod match {
            case GroupByMethod.InJVM =>
              inputCols
                .groupBy(_._1)
                .map {
                  case (groupingId, list) => {
                    (groupingId, list.map(_._2).sum)
                  }
                }

            case arrowBased: GroupByMethod.ArrowBased =>
              ArrowVectorBuilders.withDirectFloat8Vector(inputCols.map(_._1)) {groupingVec =>
                ArrowVectorBuilders.withDirectFloat8Vector(inputCols.map(_._2)) {valuesVec => {
                  arrowBased match {
                    case GroupByMethod.ArrowBased.JvmArrowBased =>
                      GroupBySum.groupBySumJVM(groupingVec, valuesVec)
                    case GroupByMethod.ArrowBased.VEBased =>
                      val alloc = new RootAllocator(Integer.MAX_VALUE)
                      val outValuesVector = new Float8Vector("values", alloc)
                      val outGroupsVector = new Float8Vector("groups", alloc)

                      GroupBySum.runOn(
                        new VeArrowNativeInterfaceNumeric(
                          Aurora4SparkExecutorPlugin._veo_proc,
                          Aurora4SparkExecutorPlugin._veo_ctx,
                          Aurora4SparkExecutorPlugin.lib
                      )
                      )(groupingVec, valuesVec, outGroupsVector, outValuesVector)
                      (0 until outGroupsVector.getValueCount)
                        .map(idx => (outGroupsVector.get(idx), outValuesVector.get(idx)))
                        .toMap
                  }
                }}
              }
          }

          resultMap.zipWithIndex.map {
            case ((groupingId, sum), idx) => {
              val writer = new UnsafeRowWriter(2)
              writer.reset()
              writer.write(0, groupingId)
              writer.write(1, sum)
              writer.getRow
            }
          }.toIterator
        })
    }

    override def output: Seq[Attribute] = Seq(
      AttributeReference("group", DoubleType, false)(),
      AttributeReference("value", DoubleType, false)()
    )
  }

  final case class GroupBySumPlanTesting(groupByMethod: GroupByMethod) extends Testing {
    type Result = (Double, Double)
    override def verifyResult(result: List[Result]): Unit = {
      assert(result.sortBy(_._1) == List((10.0, 25.0), (20.0, 0.0), (40.0, 42.0)))
    }
    override def prepareSession(): SparkSession = {
      val conf = new SparkConf()
      conf.setMaster("local")
      conf.setAppName("local-test")
      conf.set("spark.ui.enabled", "false")
      VERewriteStrategy._enabled = false
      SparkSession
        .builder()
        .config(conf)
        .config(CODEGEN_FALLBACK.key, value = false)
        .config(
          key = "spark.plugins",
          value = if (testingTarget.isVE) classOf[AuroraSqlPlugin].getCanonicalName else ""
        )
        .config("spark.sql.codegen.comments", value = true)
        .withExtensions(sse =>
          sse.injectPlannerStrategy(sparkSession =>
            new Strategy {
              override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
                plan match {
                  case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
                    List(SimpleGroupBySum(planLater(child), groupByMethod))
                  case _ => Nil
                }
              }
            }
          )
        )
        .getOrCreate()
    }

    override def prepareInput(
                               sparkSession: SparkSession,
                               dataSize: Testing.DataSize
                             ): Dataset[Result] = {
      import sparkSession.sqlContext.implicits._
      List[(Double, Double)](
        (10.0, 5.0), (10.0, 20.0), (20.0, 1.0), (40.0, 42.0), (20.0, -1.0)
      ).toDS
        .withColumnRenamed("_1", "key")
        .withColumnRenamed("_2", "value")
        .createOrReplaceTempView("a")

      import sparkSession.sqlContext.implicits._

      sparkSession
        .sql("SELECT key, sum(value) from a group by key")
        .as[(Double, Double)]
    }
    override def cleanUp(sparkSession: SparkSession): Unit = {
      sparkSession.close()
      VERewriteStrategy._enabled = true
    }
    override def testingTarget: Testing.TestingTarget = groupByMethod match {
      case GroupByMethod.VEBased       => TestingTarget.VectorEngine
      case GroupByMethod.JvmArrowBased => TestingTarget.PlainSpark
    }

  }

  final case class GroupBySumPlainSparkTesting() extends Testing {
    type Result = (Double, Double)
    override def verifyResult(data: List[Result]): Unit = {
      assert(data.sortBy(_._1) == List((10.0, 25.0), (20.0, 0.0), (40.0, 42.0)))
    }
    override def prepareInput(
                               sparkSession: SparkSession,
                               dataSize: Testing.DataSize
                             ): Dataset[(Double, Double)] = {
      import sparkSession.sqlContext.implicits._
      List[(Double, Double)](
        (10.0, 5.0), (10.0, 20.0), (20.0, 1.0), (40.0, 42.0), (20.0, -1.0)
      ).toDS
        .withColumnRenamed("_1", "key")
        .withColumnRenamed("_2", "value")
        .createOrReplaceTempView("a")

      import sparkSession.sqlContext.implicits._

      sparkSession
        .sql("SELECT key, sum(value) from a group by key")
        .as[(Double, Double)]
    }
    override def prepareSession(): SparkSession = {
      val conf = new SparkConf()
      conf.setMaster("local")
      conf.setAppName("local-test")
      conf.set("spark.ui.enabled", "false")
      SparkSession
        .builder()
        .config(conf)
        .config(CODEGEN_FALLBACK.key, value = false)
        .config("spark.sql.codegen.comments", value = true)
        .getOrCreate()
    }
    override def cleanUp(sparkSession: SparkSession): Unit = sparkSession.close()
    override def testingTarget: Testing.TestingTarget = TestingTarget.PlainSpark
  }

  val OurTesting: List[Testing] = List(
    GroupBySumPlanTesting(GroupByMethod.VEBased),
    GroupBySumPlanTesting(GroupByMethod.JvmArrowBased),
    GroupBySumPlainSparkTesting()
  )
}

final class GroupBySumPlanSpec extends AnyFreeSpec with BenchTestAdditions {
  GroupBySumPlanSpec.OurTesting.filter(_.testingTarget.isPlainSpark).foreach(runTestCase)
}
