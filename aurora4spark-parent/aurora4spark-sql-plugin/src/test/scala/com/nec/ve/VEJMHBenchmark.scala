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
  def testOneRun(): Unit = {
    val query = sparkSession.sql("SELECT SUM(a) FROM nums")
    assert(query.collect().nonEmpty)
  }

  // @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  def testJVMRun(): Unit = {
    LocalVeoExtension._enabled = false
    val query = sparkSession.sql("SELECT SUM(a) FROM nums")
    assert(query.collect().nonEmpty)
  }

  @Setup
  def prepare(): Unit = {
    Aurora4SparkExecutorPlugin.closeAutomatically = false

    this._sparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName(this.getClass.getCanonicalName)
      .config(key = SQLConf.SHUFFLE_PARTITIONS.key, value = 1)
      .config(key = SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, value = 1)
      .config(key = SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, value = "false")
      .config(key = "spark.plugins", value = classOf[AuroraSqlPlugin].getCanonicalName)
      .config(key = "spark.ui.enabled", value = false)
      .getOrCreate()

    import sparkSession.implicits._
    sparkSession.sqlContext.read
      .format("parquet")
      .load("/home/william/large-sample-parquet-10_9/")
      .createOrReplaceTempView("nums")
  }

  @TearDown
  def tearDown(): Unit = {
    sparkSession.close()
    Aurora4SparkExecutorPlugin.closeProcAndCtx()
  }

}
