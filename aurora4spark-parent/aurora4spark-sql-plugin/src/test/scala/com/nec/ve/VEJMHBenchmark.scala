package com.nec.ve

import com.nec.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.debug.DebugQuery
import org.apache.spark.sql.internal.SQLConf
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class VEJMHBenchmark {

  private var _sparkSession: SparkSession = null
  private lazy val sparkSession: SparkSession = _sparkSession

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @Fork(value = 1, warmups = 0)
  def test1VERun(): Unit = {
    val query = sparkSession.sql("SELECT MIN(a) FROM nums")
    println(s"VE result = ${query.collect().toList}")
  }

  @Benchmark
  @Fork(value = 1, warmups = 0)
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def test2JVMRun(): Unit = {
    LocalVeoExtension._enabled = false
    val query = sparkSession.sql("SELECT MIN(a) FROM nums")
    println(s"JVM result = ${query.collect().toList}")
  }

  @Setup
  def prepare(): Unit = {
    Aurora4SparkExecutorPlugin.closeAutomatically = false

    this._sparkSession = SparkSession
      .builder()
      .master("local[4]")
      .appName(this.getClass.getCanonicalName)
      .config(key = "spark.plugins", value = classOf[AuroraSqlPlugin].getCanonicalName)
      .config(key = "spark.ui.enabled", value = false)
      .config(key = "spark.sql.columnVector.offheap.enabled", value = true)
      .getOrCreate()

    import sparkSession.implicits._
    sparkSession.sqlContext.read
      .format("parquet")
      .load("/home/william/large-sample-parquet-10_8/")
      .createOrReplaceTempView("nums")
  }

  @TearDown
  def tearDown(): Unit = {
    sparkSession.close()
    Aurora4SparkExecutorPlugin.closeProcAndCtx()
  }

}
