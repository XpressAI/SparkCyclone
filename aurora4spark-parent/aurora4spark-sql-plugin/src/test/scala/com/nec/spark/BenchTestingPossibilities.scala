package com.nec.spark
import com.nec.spark.BenchTestingPossibilities.BenchTestAdditions
import com.nec.spark.BenchTestingPossibilities.Testing.DataSize
import com.nec.spark.planning.simplesum.SimpleSumPlanTest
import com.nec.spark.planning.simplesum.SimpleSumPlanTest.Source
import org.apache.spark.sql.SparkSession
import org.scalatest.freespec.AnyFreeSpec
import com.eed3si9n.expecty.Expecty.assert

object BenchTestingPossibilities {
  /** Compiler-friendly name that we can use as part of class an method names. */
  final case class CleanName(value: String) {
    override def toString: String = value
  }
  object CleanName {

    implicit class RichStringClean(string: String) {
      def clean: CleanName = fromString(string)
    }
    def fromString(value: String): CleanName = CleanName(value.replaceAll("[^a-zA-Z_0-9]", ""))
  }
  abstract class Testing {
    def name: CleanName
    def verify(sparkSession: SparkSession): Unit
    def benchmark(sparkSession: SparkSession): Unit
    def prepareSession(dataSize: DataSize = DataSize.BenchmarkSize): SparkSession
    def cleanUp(sparkSession: SparkSession): Unit
    def requiresVe: Boolean
  }

  object Testing {

    /**
     * We may prepare a session with a small amount of data, but also with a big amount of data
     *
     * This enables us to confirm the *correctness* before we proceed with heavy benchmarking.
     */
    sealed trait DataSize
    object DataSize {
      case object BenchmarkSize extends DataSize
      case object SanityCheckSize extends DataSize
    }
  }

  /** You can generate variations of these as well as well, including CSV and so forth */
  def testSql(sql: String, expectedResult: Double, source: Source): Testing = new Testing {
    override def name: CleanName = CleanName.fromString(s"${source.title}${sql}")
    override def benchmark(sparkSession: SparkSession): Unit = {
      sparkSession.sql(sql)
    }
    override def prepareSession(dataSize: DataSize): SparkSession = {
      val sess = SparkSession
        .builder()
        .master("local[4]")
        .appName(name.value)
        .config(key = "spark.ui.enabled", value = false)
        .getOrCreate()

      source.generate(sess)

      sess
    }

    override def cleanUp(sparkSession: SparkSession): Unit = sparkSession.close()
    override def verify(sparkSession: SparkSession): Unit = {
      import sparkSession.implicits._
      assert(sparkSession.sql(sql).as[Double].collect().toList == List(expectedResult))
    }
    override def requiresVe: Boolean = false
  }

  /** Proof of generating any variation of things */
  val SampleTests: List[Testing] = {
    for {
      num <- 2 to 4
      source <- List(Source.CSV, Source.Parquet)
    } yield testSql(
      sql = s"SELECT SUM(value + $num) FROM nums",
      expectedResult = 62 + 5 * num,
      source = source
    )
  }.toList

  val possibilities: List[Testing] = List(SampleTests, SimpleSumPlanTest.OurTesting).flatten

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
  BenchTestingPossibilities.SampleTests.filterNot(_.requiresVe).foreach(runTestCase)

}
