package com.nec.ve

import com.eed3si9n.expecty.Expecty.expect

import java.net.URLClassLoader
import com.nec.spark.SparkAdditions
import org.apache.log4j.Level
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec

import java.net.URL
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

  lazy val currentClasspath =
    ClassLoader
      .getSystemClassLoader()
      .asInstanceOf[URLClassLoader]
      .getURLs()

  lazy val extraClassPath =
    currentClasspath
      .filter(item => expectedItems.exists(expected => item.toString.contains(expected)))
      .map(fixPath)

  private def fixPath(item: URL): String = {
    item.toString.replaceAllLiterally("file:/C:/", "C:/").replaceAllLiterally("file:/", "/")
  }

  private lazy val agentJar: Path = Paths.get(
    currentClasspath
      .find(_.toString.contains("agent_2.11.jar"))
      .map(fixPath)
      .getOrElse(sys.error("Could not find the agent JAR"))
  )

  "Our extra classpath" - {
    expectedItems.foreach { name =>
      s"Has ${name}" in {
        expect(extraClassPath.exists(_.contains(name)))
      }
    }

    "All items begin with /, ie absolute paths" in {
      expect(extraClassPath.forall(f => f.startsWith("/") || f.startsWith("C:/")))
    }
  }

  "Agent JAR exists" in {
    assert(Files.exists(agentJar))
  }

  "We can execute in cluster-local mode" in {
    val agentJarStr = agentJar.toString
    withSparkSession2(builder =>
      builder
        .config("spark.ui.enabled", "true")
        .config("spark.master", "local-cluster[2,1,1024]")
        .config("spark.executor.extraClassPath", extraClassPath.mkString(":"))
        .config("spark.executor.extraJavaOptions", s"-javaagent:${agentJarStr}")
        .config("spark.driver.extraJavaOptions", s"-javaagent:${agentJarStr}")
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
