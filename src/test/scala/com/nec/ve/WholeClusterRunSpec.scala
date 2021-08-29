package com.nec.ve

import com.eed3si9n.expecty.Expecty.expect

import java.net.URLClassLoader
import com.nec.native.NativeCompiler
import com.nec.native.NativeEvaluator.InMemoryLibraryEvaluator
import com.nec.spark.planning.VERewriteStrategy
import com.nec.spark.{Aurora4SparkExecutorPlugin, SparkAdditions}
import org.apache.log4j.Level
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

final class WholeClusterRunSpec extends AnyFreeSpec with BeforeAndAfter with SparkAdditions {
  override protected def logLevel: Level = Level.ERROR
  val expectedItems =
    List(
      "/classes/",
      "/test-classes/",
      "aurora-presets",
      "scala-logging",
      "aveo4j",
      "javacpp",
      "jna",
      "commons-io",
      "reflections"
    )
  lazy val extraClassPath =
    ClassLoader
      .getSystemClassLoader()
      .asInstanceOf[URLClassLoader]
      .getURLs()
      .filter(item => expectedItems.exists(expected => item.toString.contains(expected)))
      .map(item => item.toString.replaceAllLiterally("file:/", "/"))

  // update with every new version of the agent
  private val agentJar: Path = Paths
    .get(scala.util.Properties.userHome)
    .resolve(".ivy2/local/com.nec.spark/agent_2.11/0.1.7/jars/agent_2.11.jar")

  "Our extra classpath" - {
    expectedItems.foreach { name =>
      s"Has ${name}" in {
        expect(extraClassPath.exists(_.contains(name)))
      }
    }

    "All items begin with /, ie absolute paths" in {
      expect(extraClassPath.forall(_.startsWith("/")))
    }
  }

  "We can execute in cluster-local mode" in {
    assert(Files.exists(agentJar), s"Expected ${agentJar} to exist.")
    withSparkSession2(builder =>
      builder
        .config("spark.ui.enabled", "true")
        .config("spark.master", "local-cluster[2,1,1024]")
        .config("spark.executor.extraClassPath", extraClassPath.mkString(":"))
        .config("spark.executor.extraJavaOptions", s"-javaagent:${agentJar}")
        .config("spark.driver.extraJavaOptions", s"-javaagent:${agentJar}")
        .config("spark.sql.extensions", "com.nec.spark.LocalVeoExtension")
        .withExtensions(sse =>
          sse.injectPlannerStrategy(sparkSession =>
            new VERewriteStrategy(
              sparkSession,
              InMemoryLibraryEvaluator(NativeCompiler.fromConfig(sparkSession.sparkContext.getConf))
            )
          )
        )
    ) { sparkSession =>
      import sparkSession.sqlContext.implicits._
      val nums = List[Double](1)
      nums
        .toDS()
        .createOrReplaceTempView("nums")
      val q = sparkSession.sql("select sum(value) from nums").as[Double]
      val result = q.collect().toList
      assert(result == List(1))
      assert(q.queryExecution.executedPlan.toString().contains("CEvaluationPlan"))
    }
  }

}
