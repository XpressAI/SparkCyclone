package com.nec.ve

import org.scalatest.freespec.AnyFreeSpec
import java.nio.file.Files
import java.nio.file.Paths
import com.nec.spark.SparkAdditions
import org.scalatest.BeforeAndAfter
import com.nec.spark.AuroraSqlPlugin
import org.apache.spark.api.resource.ResourceDiscoveryPlugin
import org.apache.spark.internal.Logging
import java.util.Optional
import org.apache.spark.SparkConf
import org.apache.spark.resource.{ResourceInformation, ResourceRequest}
import org.apache.log4j.Level
import java.net.URLClassLoader
import com.eed3si9n.expecty.Expecty._

final class DetectVectorEngineSpec extends AnyFreeSpec with BeforeAndAfter with SparkAdditions {
  "It works" in {
    assert(DiscoverVectorEnginesPlugin.detectVE() == List("0", "1"))
  }

  override protected def logLevel: Level = Level.INFO

  val expectedItems =
    List(
      "/ve-classes/",
      "/classes/",
      "/test-classes/",
      "aurora-presets",
      "aveo4j",
      "javacpp",
      "jna",
      "commons-io",
      "reflections"
    )

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

  lazy val extraClassPath =
    ClassLoader
      .getSystemClassLoader()
      .asInstanceOf[URLClassLoader]
      .getURLs()
      .filter(item => expectedItems.exists(expected => item.toString.contains(expected)))
      .map(item => item.toString.replaceAllLiterally("file:/", "/"))

  "We can execute in cluster-local mode" in withSparkSession2(
    _.config("spark.executor.resource.ve.amount", "1")
      .config("spark.task.resource.ve.amount", "1")
      .config("spark.worker.resource.ve.amount", "1")
      .config("spark.plugins", classOf[AuroraSqlPlugin].getCanonicalName)
      .config("spark.ui.enabled", "true")
      .config("spark.executor.extraClassPath", extraClassPath.mkString(":"))
      .config("spark.master", "local-cluster[2,1,1024]")
      .config(
        "spark.resources.discoveryPlugin",
        classOf[DiscoverVectorEnginesPlugin].getCanonicalName
      )
  ) { sparkSession =>
    import sparkSession.sqlContext.implicits._
    val nums = List[Double](1)
    nums
      .toDS()
      .createOrReplaceTempView("nums")
    val q = sparkSession.sql("select sum(value) from nums").as[Double]
    println(q.queryExecution.executedPlan.toString())
    println(sparkSession.sparkContext.resources)
    val result = q.collect().toList
    assert(result == List(1))
  }

}
