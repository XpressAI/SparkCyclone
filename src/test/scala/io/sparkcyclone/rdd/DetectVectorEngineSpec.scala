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
package io.sparkcyclone.rdd

import com.eed3si9n.expecty.Expecty._
import io.sparkcyclone.annotations.VectorEngineTest
import io.sparkcyclone.spark.{AuroraSqlPlugin, SparkAdditions}
import io.sparkcyclone.plugin.DiscoverVectorEnginesPlugin
import io.sparkcyclone.rdd.DetectVectorEngineSpec.{ExpectedClassPathItems, ExtraClassPath, VeClusterConfig}
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec

import java.net.URLClassLoader
import java.nio.file.{Files, Paths}


object DetectVectorEngineSpec {
  private val ExpectedClassPathItems =
    List(
      "/classes/",
      "/test-classes/",
      "scala-logging",
      "veoffload",
      "javacpp",
      "jna",
      "commons-io",
      "haoyi"
    )

  private lazy val ExtraClassPath =
    ClassLoader.getSystemClassLoader
      .asInstanceOf[URLClassLoader]
      .getURLs
      .filter(item => ExpectedClassPathItems.exists(expected => item.toString.contains(expected)))
      .map(item => item.toString.replaceAllLiterally("file:/", "/"))
      .toList

  val VeClusterConfig: SparkSession.Builder => SparkSession.Builder =
    _.config("spark.executor.resource.ve.amount", "1")
      .config("spark.task.resource.ve.amount", "1")
      .config("spark.worker.resource.ve.amount", "1")
      .config("spark.plugins", classOf[AuroraSqlPlugin].getCanonicalName)
      .config("spark.ui.enabled", "true")
      .config("spark.executor.extraClassPath", ExtraClassPath.mkString(":"))
      .config("spark.master", "local-cluster[2,1,1024]")
      .config(
        "spark.resources.discoveryPlugin",
        classOf[DiscoverVectorEnginesPlugin].getCanonicalName
      )

  final case class SampleClass(a: Int, b: Double)
}

@VectorEngineTest
final class DetectVectorEngineSpec extends AnyFreeSpec with BeforeAndAfter with SparkAdditions {
  "It works" in {
    import scala.collection.JavaConverters._

    // Check using a different regex.
    val regex = "^veslot[0-7]$".r
    val veSlots = Files
      .list(Paths.get("/dev/"))
      .iterator()
      .asScala
      .filter(path => regex.unapplySeq(path.getFileName.toString).nonEmpty)
      .map(_.toString.drop(11))
      .toList
      .sorted
    assert(DiscoverVectorEnginesPlugin.detectVE() == veSlots)
  }

  override protected def logLevel: Level = Level.ERROR
  // override protected def logLevel: Level = Level.INFO

  "Our extra classpath" - {
    ExpectedClassPathItems.foreach { name =>
      s"Classpath: Has ${name}" in {
        expect(ExtraClassPath.exists(_.contains(name)))
      }
    }

    "Classpath: All items begin with /, ie absolute paths" in {
      expect(ExtraClassPath.forall(_.startsWith("/")))
    }
  }

  "We can execute in cluster-local mode and it doesn't crash" ignore withSparkSession2(
    VeClusterConfig
      .andThen(DynamicVeSqlExpressionEvaluationSpec.VeConfiguration)
      .andThen(_.config("spark.cyclone.sql.exchange-on-ve", "true"))
  ) { sparkSession =>
    import sparkSession.sqlContext.implicits._
    val nums =
      List(
        DetectVectorEngineSpec.SampleClass(1, 5.0),
        DetectVectorEngineSpec.SampleClass(2, 6.0),
        DetectVectorEngineSpec.SampleClass(1, 9)
      )
    nums
      .toDS()
      .createOrReplaceTempView("nums")
    val q = sparkSession.sql("select sum(b) from nums group by a").as[Double]
    println(q.queryExecution.executedPlan)
    val result = q.collect().toSet
    assert(result == Set[Double](14, 6.0))
  }

}
