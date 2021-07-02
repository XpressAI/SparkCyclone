package com.nec.spark
import com.nec.spark.BenchTestingPossibilities.BenchTestAdditions
import com.nec.spark.planning.simplesum.SimpleSumPlanTest.Source
import org.apache.spark.sql.SparkSession
import org.scalatest.freespec.AnyFreeSpec
import com.eed3si9n.expecty.Expecty.assert
import com.nec.spark.agile.CleanName
import com.nec.spark.planning.simplesum.JoinPlanSpec
import com.nec.testing.Testing
import com.nec.testing.Testing.DataSize
import com.nec.testing.Testing.TestingTarget

object BenchTestingPossibilities {

  /** You can generate variations of these as well as well, including CSV and so forth */
  final case class SimpleSql(sql: String, expectedResult: Double, source: Source) extends Testing {
    override def benchmark(sparkSession: SparkSession): Unit = {
      sparkSession.sql(sql).collect()
    }
    override def prepareSession(dataSize: DataSize): SparkSession = {
      val sess = SparkSession
        .builder()
        .master("local[4]")
        .appName(name.value)
        .config(key = "spark.ui.enabled", value = false)
        .getOrCreate()

      source.generate(sess, dataSize)

      sess
    }

    override def verify(sparkSession: SparkSession): Unit = {
      import sparkSession.implicits._
      assert(sparkSession.sql(sql).as[Double].collect().toList == List(expectedResult))
    }
    override def testingTarget: Testing.TestingTarget = TestingTarget.PlainSpark
  }

  final case class SimpleVE(sql: String, expectedResult: Double, source: Source) extends Testing {
    override def benchmark(sparkSession: SparkSession): Unit = {
      sparkSession.sql(sql).collect()
    }

    override def prepareSession(dataSize: DataSize): SparkSession = {
      LocalVeoExtension._enabled = true

      val sess = SparkSession
        .builder()
        .master("local[4]")
        .appName(name.value)
        .config(key = "spark.ui.enabled", value = false)
        .config(key = "spark.plugins", value = classOf[AuroraSqlPlugin].getCanonicalName)
        .config(key = "spark.ui.enabled", value = false)
        .config(key = "spark.sql.columnVector.offheap.enabled", value = true)
        .config(
          key = org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key,
          value = false
        )
        .getOrCreate()

      source.generate(sess, dataSize)

      sess
    }

    override def cleanUp(sparkSession: SparkSession): Unit = {
      sparkSession.close()
      Aurora4SparkExecutorPlugin.closeProcAndCtx()
    }

    override def verify(sparkSession: SparkSession): Unit = {
      import sparkSession.implicits._
      sparkSession.sql(sql).as[Double].collect().toList == List(expectedResult)
    }
    override def testingTarget: Testing.TestingTarget = TestingTarget.VectorEngine
  }

  final case class SimpleSqlRapids(sql: String, expectedResult: Double, source: Source)
    extends Testing {
    override def benchmark(sparkSession: SparkSession): Unit = {
      val result = sparkSession.sql(sql)
      println(result.queryExecution.executedPlan)
      result.collect()
    }
    override def prepareSession(dataSize: DataSize): SparkSession = {
      val sess = SparkSession
        .builder()
        .master("local[4]")
        .appName(name.value)
        .config(key = "spark.ui.enabled", value = false)
        .config(key = "spark.plugins", value = "com.nvidia.spark.SQLPlugin")
        .config(key = "spark.rapids.sql.concurrentGpuTasks", 1)
        .config(key = "spark.rapids.sql.variableFloatAgg.enabled", "true")
        .getOrCreate()

      source.generate(sess, dataSize)

      sess
    }

    override def cleanUp(sparkSession: SparkSession): Unit = sparkSession.close()
    override def verify(sparkSession: SparkSession): Unit = {
      import sparkSession.implicits._
      sparkSession.sql(sql).as[Double].collect().toList == List(expectedResult)
    }
    override def testingTarget: Testing.TestingTarget = TestingTarget.Rapids
  }

  final case class SqlVeWholestageCodegen(sql: String, expectedResult: Double, source: Source)
    extends Testing {
    override def benchmark(sparkSession: SparkSession): Unit = {
      val result = sparkSession.sql(sql)
      println(result.queryExecution.executedPlan)
      result.collect()
    }

    override def prepareSession(dataSize: DataSize): SparkSession = {
      LocalVeoExtension._enabled = true
      LocalVeoExtension._useCodegenPlans = true
      val sess = SparkSession
        .builder()
        .master("local[4]")
        .appName(name.value)
        .config(key = "spark.ui.enabled", value = false)
        .config(key = "spark.plugins", value = classOf[AuroraSqlPlugin].getCanonicalName)
        .config(key = "spark.ui.enabled", value = false)
        .config(key = "spark.sql.columnVector.offheap.enabled", value = true)
        .config(
          key = org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key,
          value = true
        )
        .getOrCreate()

      source.generate(sess, dataSize)

      sess
    }

    override def cleanUp(sparkSession: SparkSession): Unit = {
      sparkSession.close()
      Aurora4SparkExecutorPlugin.closeProcAndCtx()
    }

    override def verify(sparkSession: SparkSession): Unit = {
      import sparkSession.implicits._
      sparkSession.sql(sql).as[Double].collect().toList == List(expectedResult)
    }
    override def testingTarget: Testing.TestingTarget = TestingTarget.VectorEngine
  }

  /** Proof of generating any variation of things */
  val SampleTests: List[Testing] = {
    for {
      num <- 2 to 4
      source <- List(Source.CSV, Source.Parquet)
    } yield SimpleSql(
      sql = s"SELECT SUM(value + $num) FROM nums",
      expectedResult = 62 + 5 * num,
      source = source
    )
  }.toList

  val possibilities: List[Testing] = {
    ((for {
      num <- 2 to 4
      source <- List(Source.CSV, Source.Parquet)
    } yield SimpleSql(
      sql = s"SELECT SUM(value + $num) FROM nums",
      expectedResult = num + 2,
      source = source
    )) ++ (for {
      source <- List(Source.CSV, Source.Parquet)
    } yield SimpleVE(
      sql = "SELECT SUM(value) FROM nums",
      expectedResult = 0,
      source = source
    )) ++ (for {
      source <- List(Source.CSV, Source.Parquet)
    } yield SimpleSqlRapids(
      sql = "SELECT SUM(value) FROM nums",
      expectedResult = 0,
      source = source
    )) ++ (
      for {
        source <- List(Source.CSV, Source.Parquet)
      } yield SqlVeWholestageCodegen(
        sql = "SELECT SUM(value) FROM nums",
        expectedResult = 0,
        source = source
      )
    )).toList
  } ++ JoinPlanSpec.OurTesting

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
  BenchTestingPossibilities.SampleTests.filter(_.testingTarget.isPlainSpark).foreach(runTestCase)

}
