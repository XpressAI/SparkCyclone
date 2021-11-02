/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.nec.spark.planning

import com.eed3si9n.expecty.Expecty.assert
import com.nec.arrow.WithTestAllocator
import com.nec.arrow.ArrowVectorBuilders
import com.nec.arrow.VeArrowNativeInterface
import com.nec.arrow.functions.Join
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.nec.spark.AuroraSqlPlugin
import com.nec.spark.BenchTestingPossibilities.BenchTestAdditions
import com.nec.spark.planning.JoinPlanSpec.OurSimpleJoin.JoinMethod
import com.nec.testing.Testing
import com.nec.testing.Testing.TestingTarget
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
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
                  WithTestAllocator { alloc =>
                    val outVector = new Float8Vector("value", alloc)
                    try ArrowVectorBuilders.withDirectFloat8Vector(leftSide.map(_._2)) {
                      firstColumnVec =>
                        ArrowVectorBuilders.withDirectFloat8Vector(rightSide.map(_._2)) {
                          secondColumnVec =>
                            ArrowVectorBuilders.withDirectIntVector(leftSide.map(_._1)) {
                              firstKeysVec =>
                                ArrowVectorBuilders.withDirectIntVector(rightSide.map(_._1)) {
                                  secondKeysVec =>
                                    jm match {
                                      case JoinMethod.ArrowBased.VEBased =>
                                        Join.runOn(
                                          new VeArrowNativeInterface(
                                            Aurora4SparkExecutorPlugin._veo_proc,
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
                    } finally outVector.close()
                  }
              }
            }
            .take(1)
            .flatMap(_.flatten)
      }
    }
    override def output: Seq[Attribute] = left.output.takeRight(1) ++ right.output.takeRight(1)
  }

  final case class TestingOUR(joinMethod: JoinMethod) extends Testing {
    type Result = (Double, Double)
    override def verifyResult(result: List[Result]): Unit = {
      assert(result.sortBy(_._1) == List((1.0, 2.0), (2.0, 3.0)))
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
                  case logical.Join(left, right, joinType, condition, hint) =>
                    require(joinType == Inner)
                    List(OurSimpleJoin(planLater(left), planLater(right), joinMethod))
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
      List[(Int, Double)](1 -> 1.0, 2 -> 2.0).toDS
        .withColumnRenamed("_1", "key")
        .withColumnRenamed("_2", "value")
        .createOrReplaceTempView("a")

      List[(Int, Double)](1 -> 2.0, 2 -> 3.0).toDS
        .withColumnRenamed("_1", "key")
        .withColumnRenamed("_2", "value")
        .createOrReplaceTempView("b")

      import sparkSession.sqlContext.implicits._

      sparkSession
        .sql("SELECT a.value, b.value FROM a INNER JOIN b ON a.key = b.key")
        .as[(Double, Double)]
    }
    override def cleanUp(sparkSession: SparkSession): Unit = {
      sparkSession.close()
      VERewriteStrategy._enabled = true
    }
    override def testingTarget: Testing.TestingTarget = joinMethod match {
      case JoinMethod.InJVM                    => TestingTarget.PlainSpark
      case JoinMethod.ArrowBased.VEBased       => TestingTarget.VectorEngine
      case JoinMethod.ArrowBased.JvmArrowBased => TestingTarget.PlainSpark
    }

  }

  final case class TestingSimpleJoinSparkJVM() extends Testing {
    type Result = (Double, Double)
    override def verifyResult(data: List[Result]): Unit = {
      assert(data.sortBy(_._1) == List((1.0, 2.0), (2.0, 3.0)))
    }
    override def prepareInput(
      sparkSession: SparkSession,
      dataSize: Testing.DataSize
    ): Dataset[(Double, Double)] = {
      import sparkSession.sqlContext.implicits._

      List[(Int, Double)](1 -> 1.0, 2 -> 2.0).toDS
        .withColumnRenamed("_1", "key")
        .withColumnRenamed("_2", "value")
        .createOrReplaceTempView("a")

      List[(Int, Double)](1 -> 2.0, 2 -> 3.0).toDS
        .withColumnRenamed("_1", "key")
        .withColumnRenamed("_2", "value")
        .createOrReplaceTempView("b")

      sparkSession
        .sql("SELECT a.value, b.value FROM a INNER JOIN b ON a.key = b.key")
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
    TestingOUR(JoinMethod.InJVM),
    // Disabled - uses old approach of loading the lib rather than the new one
    // TestingOUR(JoinMethod.ArrowBased.VEBased),
    TestingOUR(JoinMethod.ArrowBased.JvmArrowBased),
    TestingSimpleJoinSparkJVM()
  )
}

final class JoinPlanSpec extends AnyFreeSpec with BenchTestAdditions {
  JoinPlanSpec.OurTesting.filter(_.testingTarget.isPlainSpark).foreach(runTestCase)
}
