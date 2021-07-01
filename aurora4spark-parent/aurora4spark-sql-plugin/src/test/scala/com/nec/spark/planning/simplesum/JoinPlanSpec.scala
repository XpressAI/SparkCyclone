package com.nec.spark.planning.simplesum
import com.eed3si9n.expecty.Expecty.assert
import com.nec.arrow.ArrowVectorBuilders
import com.nec.arrow.VeArrowNativeInterfaceNumeric
import com.nec.arrow.functions.Join
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.nec.spark.AuroraSqlPlugin
import com.nec.spark.BenchTestingPossibilities.BenchTestAdditions
import com.nec.spark.BenchTestingPossibilities.Testing
import com.nec.spark.agile.CleanName
import com.nec.spark.agile.CleanName.RichStringClean
import com.nec.spark.planning.simplesum.JoinPlanSpec.OurSimpleJoin.JoinMethod
import com.nec.spark.planning.simplesum.SimpleSumPlanTest.RichDataSet
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.BinaryExecNode
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.scalatest.freespec.AnyFreeSpec

import java.nio.file.Paths
import com.nec.ve.VeKernelCompiler

object JoinPlanSpec {
  object OurSimpleJoin {
    sealed trait JoinMethod extends Serializable
    object JoinMethod {
      case object InJVM extends JoinMethod
      sealed trait ArrowBased extends JoinMethod
      object ArrowBased {
        case object JvmArrowBased extends ArrowBased
        case object VEBased extends ArrowBased
      }
    }
  }
  final case class OurSimpleJoin(left: SparkPlan, right: SparkPlan, joinMethod: JoinMethod)
    extends BinaryExecNode {
    override protected def doExecute(): RDD[InternalRow] = {
      left.execute().coalesce(1).zipPartitions(right.execute().coalesce(1)) {
        case (leftIter, rightIter) =>
          /** highly inefficient implementation! for testing only */
          Iterator
            .continually {
              val leftSide = leftIter.map(ir => (ir.getInt(0), ir.getDouble(1))).toList
              val rightSide = rightIter.map(ir => (ir.getInt(0), ir.getDouble(1))).toList

              joinMethod match {
                case JoinMethod.InJVM =>
                  for {
                    (lk, lv) <- leftSide
                    (rk, rv) <- rightSide
                    if lk == rk
                  } yield {
                    val writer = new UnsafeRowWriter(2)
                    writer.reset()
                    writer.write(0, lv)
                    writer.write(1, rv)
                    Iterator(writer.getRow)
                  }
                case jm: JoinMethod.ArrowBased =>
                  val alloc = new RootAllocator(Integer.MAX_VALUE)
                  val outVector = new Float8Vector("value", alloc)
                  ArrowVectorBuilders.withDirectFloat8Vector(leftSide.map(_._2)) { firstColumnVec =>
                    ArrowVectorBuilders.withDirectFloat8Vector(rightSide.map(_._2)) {
                      secondColumnVec =>
                        ArrowVectorBuilders.withDirectIntVector(leftSide.map(_._1)) {
                          firstKeysVec =>
                            ArrowVectorBuilders.withDirectIntVector(rightSide.map(_._1)) {
                              secondKeysVec =>
                                jm match {
                                  case JoinMethod.ArrowBased.VEBased =>
                                    Join.runOn(
                                      new VeArrowNativeInterfaceNumeric(
                                        Aurora4SparkExecutorPlugin._veo_proc,
                                        Aurora4SparkExecutorPlugin._veo_ctx,
                                        Aurora4SparkExecutorPlugin.lib
                                      )
                                    )(
                                      firstColumnVec,
                                      secondColumnVec,
                                      firstKeysVec,
                                      secondKeysVec,
                                      outVector
                                    )

                                    val res = (0 until outVector.getValueCount)
                                      .map(i => outVector.get(i))
                                      .toList
                                      .splitAt(outVector.getValueCount / 2)
                                    res._1
                                      .zip(res._2)
                                      .map { case (a, b) =>
                                        val writer = new UnsafeRowWriter(2)
                                        writer.reset()
                                        writer.write(0, a)
                                        writer.write(1, b)
                                        Iterator(writer.getRow)
                                      }

                                  case JoinMethod.ArrowBased.JvmArrowBased =>
                                    Join
                                      .joinJVM(
                                        firstColumnVec,
                                        secondColumnVec,
                                        firstKeysVec,
                                        secondKeysVec
                                      )
                                      .map { case (a, b) =>
                                        val writer = new UnsafeRowWriter(2)
                                        writer.reset()
                                        writer.write(0, a)
                                        writer.write(1, b)
                                        Iterator(writer.getRow)
                                      }
                                }
                            }
                        }
                    }
                  }
              }
            }
            .take(1)
            .flatMap(_.flatten)
      }
    }
    override def output: Seq[Attribute] = left.output.takeRight(1) ++ right.output.takeRight(1)
  }

  def testingOur(joinMethod: JoinMethod): Testing = new Testing {
    override def name: CleanName = s"SimpleJoin_${joinMethod}".clean
    override def verify(sparkSession: SparkSession): Unit = {
      import sparkSession.implicits._
      val ds = sparkSession
        .sql("SELECT a.value, b.value FROM a INNER JOIN b ON a.key = b.key")
        .debugSqlHere
        .as[(Double, Double)]
      val result = ds.collect().toList
      assert(result.sortBy(_._1) == List((1.0, 2.0), (2.0, 3.0)))
    }
    override def benchmark(sparkSession: SparkSession): Unit = {
      verify(sparkSession)
    }
    override def prepareSession(dataSize: Testing.DataSize): SparkSession = {
      val conf = new SparkConf()
      conf.setMaster("local")
      conf.setAppName("local-test")
      conf.set("spark.ui.enabled", "false")
      val ss = SparkSession
        .builder()
        .config(conf)
        .config(CODEGEN_FALLBACK.key, value = false)
        .config(
          key = "spark.plugins",
          value = if (requiresVe) classOf[AuroraSqlPlugin].getCanonicalName else ""
        )
        .config("spark.sql.codegen.comments", value = true)
        .withExtensions(sse =>
          sse.injectPlannerStrategy(sparkSession =>
            new Strategy {
              override def apply(plan: LogicalPlan): Seq[SparkPlan] =
                plan match {
                  case logical.Join(left, right, joinType, condition, hint) =>
                    require(joinType == Inner)
                    List(OurSimpleJoin(planLater(left), planLater(right), joinMethod))
                  case _ => Nil
                }
            }
          )
        )
        .getOrCreate()
      import ss.sqlContext.implicits._
      List[(Int, Double)](1 -> 1.0, 2 -> 2.0).toDS
        .withColumnRenamed("_1", "key")
        .withColumnRenamed("_2", "value")
        .createOrReplaceTempView("a")

      List[(Int, Double)](1 -> 2.0, 2 -> 3.0).toDS
        .withColumnRenamed("_1", "key")
        .withColumnRenamed("_2", "value")
        .createOrReplaceTempView("b")
      ss
    }
    override def cleanUp(sparkSession: SparkSession): Unit = sparkSession.close()

    override def requiresVe: Boolean = joinMethod match {
      case JoinMethod.InJVM                    => false
      case JoinMethod.ArrowBased.VEBased       => true
      case JoinMethod.ArrowBased.JvmArrowBased => false
    }

  }

  val testingSparkJVM: Testing = new Testing {
    override def name: CleanName = s"SimpleJoin_Spark".clean
    override def verify(sparkSession: SparkSession): Unit = {
      import sparkSession.implicits._
      val ds = sparkSession
        .sql("SELECT a.value, b.value FROM a INNER JOIN b ON a.key = b.key")
        .debugSqlHere
        .as[(Double, Double)]
      val result = ds.collect().toList
      assert(result.sortBy(_._1) == List((1.0, 2.0), (2.0, 3.0)))
    }
    override def benchmark(sparkSession: SparkSession): Unit = {
      verify(sparkSession)
    }
    override def prepareSession(dataSize: Testing.DataSize): SparkSession = {
      val conf = new SparkConf()
      conf.setMaster("local")
      conf.setAppName("local-test")
      conf.set("spark.ui.enabled", "false")
      val ss = SparkSession
        .builder()
        .config(conf)
        .config(CODEGEN_FALLBACK.key, value = false)
        .config("spark.sql.codegen.comments", value = true)
        .getOrCreate()
      import ss.sqlContext.implicits._
      List[(Int, Double)](1 -> 1.0, 2 -> 2.0).toDS
        .withColumnRenamed("_1", "key")
        .withColumnRenamed("_2", "value")
        .createOrReplaceTempView("a")

      List[(Int, Double)](1 -> 2.0, 2 -> 3.0).toDS
        .withColumnRenamed("_1", "key")
        .withColumnRenamed("_2", "value")
        .createOrReplaceTempView("b")
      ss
    }
    override def cleanUp(sparkSession: SparkSession): Unit = sparkSession.close()
    override def requiresVe: Boolean = false
  }
  val OurTesting: List[Testing] = List(
    testingOur(JoinMethod.InJVM),
    testingOur(JoinMethod.ArrowBased.VEBased),
    testingOur(JoinMethod.ArrowBased.JvmArrowBased),
    testingSparkJVM
  )
}

final class JoinPlanSpec extends AnyFreeSpec with BenchTestAdditions {
  JoinPlanSpec.OurTesting.filterNot(_.requiresVe).foreach(runTestCase)
}
