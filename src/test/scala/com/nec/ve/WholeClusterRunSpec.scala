package com.nec.ve


import java.net.URLClassLoader

import com.nec.native.NativeCompiler
import com.nec.native.NativeEvaluator.InMemoryLibraryEvaluator
import com.nec.spark.planning.VERewriteStrategy
import com.nec.spark.{Aurora4SparkExecutorPlugin, SparkAdditions}
import org.apache.log4j.Level
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec

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

  "We can execute in cluster-local mode" in withSparkSession2(builder =>

    builder
      .config("spark.ui.enabled", "true")
      .config("spark.master", "local-cluster[2,1,1024]")
      .config("spark.executor.extraClassPath", extraClassPath.mkString(":"))
      .config("spark.executor.extraJavaOptions","-javaagent:/home/dominik/aurora4spark/agent-base/target/scala-2.11/agent-base-assembly-0.1.0-SNAPSHOT.jar")
      .config("spark.driver.extraJavaOptions","-javaagent:/home/dominik/aurora4spark/agent-base/target/scala-2.11/agent-base-assembly-0.1.0-SNAPSHOT.jar")

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
