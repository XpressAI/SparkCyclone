//package com.nec.spark.planning
//
//import com.eed3si9n.expecty.Expecty.assert
//import com.nec.arrow.functions.Join
//import com.nec.arrow.{ArrowVectorBuilders, VeArrowNativeInterfaceNumeric}
//import com.nec.spark.BenchTestingPossibilities.BenchTestAdditions
//import com.nec.spark.planning.GroupPlanSpec.SimpleGroupBy.GroupByMethod
//import com.nec.spark.planning.JoinPlanSpec.OurSimpleJoin.JoinMethod
//import com.nec.spark.{Aurora4SparkExecutorPlugin, AuroraSqlPlugin}
//import com.nec.testing.Testing
//import com.nec.testing.Testing.TestingTarget
//import org.apache.arrow.memory.RootAllocator
//import org.apache.arrow.vector.Float8Vector
//import org.scalatest.freespec.AnyFreeSpec
//
//import org.apache.spark.SparkConf
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.catalyst.InternalRow
//import org.apache.spark.sql.catalyst.expressions.Attribute
//import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
//import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
//import org.apache.spark.sql.catalyst.plans.{Inner, logical}
//import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
//import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
//import org.apache.spark.sql.{Dataset, SparkSession, Strategy}
//
//object GroupPlanSpec {
//  object SimpleGroupBy {
//    sealed trait GroupByMethod extends Serializable
//    object GroupByMethod {
//      case object InJVM extends GroupByMethod
//      sealed trait ArrowBased extends GroupByMethod
//      object ArrowBased {
//        case object JvmArrowBased extends ArrowBased
//        case object VEBased extends ArrowBased
//      }
//    }
//  }
//  final case class SimpleGroupBy(child: SparkPlan, groupMethod: GroupByMethod)
//    extends BinaryExecNode {
//    override protected def doExecute(): RDD[InternalRow] = {
//      child
//        .execute()
//        .coalesce(1)
//        .mapPartitions(iterator => {
//              groupMethod match {
//                case GroupByMethod.InJVM =>
//                  val list = iterator.map(row => (row.getDouble(0), row.getDouble(1)))
//                  val writer = new UnsafeRowWriter(2)
//                  writer.reset()
//                  val d = list
//                    .toList
//                    .groupBy(_._1)
//                    .zipWithIndex
//                    .map {
//                      case ((groupingId, list), idx) => {
//                        writer.write(idx * 2, groupingId)
//                        writer.write((idx * 2) + 1, list.map(_._2).sum)
//                        val row = writer.getRow
//                        writer.resetRowWriter()
//                        row
//                      }
//                    }
//                  d.toIterator
//                case ArrowBase
//
//      }
//    })
//    }
//    override def output: Seq[Attribute] = left.output.takeRight(1) ++ right.output.takeRight(1)
//  }
//
//  final case class TestingOUR(joinMethod: JoinMethod) extends Testing {
//    type Result = (Double, Double)
//    override def verifyResult(result: List[Result]): Unit = {
//      assert(result.sortBy(_._1) == List((1.0, 2.0), (2.0, 3.0)))
//    }
//    override def prepareSession(): SparkSession = {
//      val conf = new SparkConf()
//      conf.setMaster("local")
//      conf.setAppName("local-test")
//      conf.set("spark.ui.enabled", "false")
//      VERewriteStrategy._enabled = false
//      SparkSession
//        .builder()
//        .config(conf)
//        .config(CODEGEN_FALLBACK.key, value = false)
//        .config(
//          key = "spark.plugins",
//          value = if (testingTarget.isVE) classOf[AuroraSqlPlugin].getCanonicalName else ""
//        )
//        .config("spark.sql.codegen.comments", value = true)
//        .withExtensions(sse =>
//          sse.injectPlannerStrategy(sparkSession =>
//            new Strategy {
//              override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
//                plan match {
//                  case logical.Join(left, right, joinType, condition, hint) =>
//                    require(joinType == Inner)
//                    List(SimpleGroupBy(planLater(left), planLater(right), joinMethod))
//                  case _ => Nil
//                }
//              }
//            }
//          )
//        )
//        .getOrCreate()
//    }
//
//    override def prepareInput(
//                               sparkSession: SparkSession,
//                               dataSize: Testing.DataSize
//                             ): Dataset[Result] = {
//      import sparkSession.sqlContext.implicits._
//      List[(Int, Double)](1 -> 1.0, 2 -> 2.0).toDS
//        .withColumnRenamed("_1", "key")
//        .withColumnRenamed("_2", "value")
//        .createOrReplaceTempView("a")
//
//      List[(Int, Double)](1 -> 2.0, 2 -> 3.0).toDS
//        .withColumnRenamed("_1", "key")
//        .withColumnRenamed("_2", "value")
//        .createOrReplaceTempView("b")
//
//      import sparkSession.sqlContext.implicits._
//
//      sparkSession
//        .sql("SELECT a.value, b.value FROM a INNER JOIN b ON a.key = b.key")
//        .as[(Double, Double)]
//    }
//    override def cleanUp(sparkSession: SparkSession): Unit = {
//      sparkSession.close()
//      VERewriteStrategy._enabled = true
//    }
//    override def testingTarget: Testing.TestingTarget = joinMethod match {
//      case JoinMethod.InJVM                    => TestingTarget.PlainSpark
//      case JoinMethod.ArrowBased.VEBased       => TestingTarget.VectorEngine
//      case JoinMethod.ArrowBased.JvmArrowBased => TestingTarget.PlainSpark
//    }
//
//  }
//
//  final case class TestingSimpleJoinSparkJVM() extends Testing {
//    type Result = (Double, Double)
//    override def verifyResult(data: List[Result]): Unit = {
//      assert(data.sortBy(_._1) == List((1.0, 2.0), (2.0, 3.0)))
//    }
//    override def prepareInput(
//                               sparkSession: SparkSession,
//                               dataSize: Testing.DataSize
//                             ): Dataset[(Double, Double)] = {
//      import sparkSession.sqlContext.implicits._
//
//      List[(Int, Double)](1 -> 1.0, 2 -> 2.0).toDS
//        .withColumnRenamed("_1", "key")
//        .withColumnRenamed("_2", "value")
//        .createOrReplaceTempView("a")
//
//      List[(Int, Double)](1 -> 2.0, 2 -> 3.0).toDS
//        .withColumnRenamed("_1", "key")
//        .withColumnRenamed("_2", "value")
//        .createOrReplaceTempView("b")
//
//      sparkSession
//        .sql("SELECT a.value, b.value FROM a INNER JOIN b ON a.key = b.key")
//        .as[(Double, Double)]
//    }
//    override def prepareSession(): SparkSession = {
//      val conf = new SparkConf()
//      conf.setMaster("local")
//      conf.setAppName("local-test")
//      conf.set("spark.ui.enabled", "false")
//      SparkSession
//        .builder()
//        .config(conf)
//        .config(CODEGEN_FALLBACK.key, value = false)
//        .config("spark.sql.codegen.comments", value = true)
//        .getOrCreate()
//    }
//    override def cleanUp(sparkSession: SparkSession): Unit = sparkSession.close()
//    override def testingTarget: Testing.TestingTarget = TestingTarget.PlainSpark
//  }
//  val OurTesting: List[Testing] = List(
//    TestingOUR(JoinMethod.InJVM),
//    TestingOUR(JoinMethod.ArrowBased.VEBased),
//    TestingOUR(JoinMethod.ArrowBased.JvmArrowBased),
//    TestingSimpleJoinSparkJVM()
//  )
//}
//
//final class JoinPlanSpec extends AnyFreeSpec with BenchTestAdditions {
//  JoinPlanSpec.OurTesting.filter(_.testingTarget.isPlainSpark).foreach(runTestCase)
//}
