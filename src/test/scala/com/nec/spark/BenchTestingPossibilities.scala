package com.nec.spark
import com.nec.spark.BenchTestingPossibilities.BenchTestAdditions
import com.nec.spark.planning.simplesum.SimpleSumPlanTest.Source
import org.apache.spark.sql.SparkSession
import org.scalatest.freespec.AnyFreeSpec
import com.nec.spark.planning.simplesum.JoinPlanSpec
import com.nec.testing.Testing
import com.nec.testing.Testing.DataSize
import com.nec.testing.Testing.TestingTarget

object BenchTestingPossibilities {

  final case class SimpleSql(
    sql: String,
    expectedResult: Double,
    source: Source,
    testingTarget: TestingTarget
  ) extends Testing {
    override def benchmark(sparkSession: SparkSession): Unit = {
      val result = sparkSession.sql(sql)
      println(result.queryExecution.executedPlan)
      result.collect()
    }
    override def prepareSession(dataSize: DataSize): SparkSession = {

      val sess = testingTarget match {
        case TestingTarget.Rapids =>
          SparkSession
            .builder()
            .master("local[4]")
            .appName(name.value)
            .config(key = "spark.ui.enabled", value = false)
            .config(key = "spark.plugins", value = "com.nvidia.spark.SQLPlugin")
            .config(key = "spark.rapids.sql.concurrentGpuTasks", 1)
            .config(key = "spark.rapids.sql.variableFloatAgg.enabled", "true")
            .getOrCreate()
        case TestingTarget.VectorEngine =>
          LocalVeoExtension._enabled = true
          SparkSession
            .builder()
            .master("local[4]")
            .appName(name.value)
            .config(key = "spark.ui.enabled", value = false)
            .config(key = "spark.plugins", value = classOf[AuroraSqlPlugin].getCanonicalName)
            .config(key = "spark.sql.columnVector.offheap.enabled", value = true)
            .getOrCreate()
        case TestingTarget.PlainSpark =>
          SparkSession
            .builder()
            .master("local[4]")
            .appName(name.value)
            .config(key = "spark.ui.enabled", value = false)
            .getOrCreate()
        case TestingTarget.CMake =>
          sys.error("Not supported")
      }

      source.generate(sess, dataSize)

      sess
    }

    override def cleanUp(sparkSession: SparkSession): Unit = sparkSession.close()
    override def verify(sparkSession: SparkSession): Unit = {
      import sparkSession.implicits._
      sparkSession.sql(sql).as[Double].collect().toList == List(expectedResult)
    }
  }

  val possibilities: List[Testing] =
    List(
      for {
        source <- List(Source.CSV, Source.Parquet)
        testingTarget <- List(
          TestingTarget.VectorEngine,
          TestingTarget.PlainSpark,
          TestingTarget.Rapids
        )
      } yield SimpleSql(
        sql = s"SELECT SUM(value) FROM nums",
        expectedResult = 123,
        source = source,
        testingTarget = testingTarget
      ),
      JoinPlanSpec.OurTesting
    ).flatten

  trait BenchTestAdditions { this: AnyFreeSpec =>
    def runTestCase(testing: Testing): Unit = {
      testing.name.value in {
        val sparkSession = testing.prepareSession(dataSize = DataSize.SanityCheckSize)
        try testing.verify(sparkSession)
        finally testing.cleanUp(sparkSession)
      }
    }
  }
}

final class BenchTestingPossibilities extends AnyFreeSpec with BenchTestAdditions {

  /** TODO We could also generate Spark plan details from here for easy cross-referencing, as well as codegen */
  BenchTestingPossibilities.possibilities.filter(_.testingTarget.isPlainSpark).foreach(runTestCase)

}
