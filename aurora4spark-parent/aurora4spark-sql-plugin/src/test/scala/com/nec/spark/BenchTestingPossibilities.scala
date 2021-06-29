package com.nec.spark
import com.nec.spark.BenchTestingPossibilities.Testing.DataSize
import com.nec.spark.planning.simplesum.SimpleSumPlanTest.Source
import org.apache.spark.sql.SparkSession
import org.scalatest.freespec.AnyFreeSpec

object BenchTestingPossibilities {
  abstract class Testing {
    def name: String
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
    override def name: String = s"${source.title}${sql.replaceAll("[^a-zA-Z_0-9]", "")}"
    override def benchmark(sparkSession: SparkSession): Unit = {
      sparkSession.sql(sql)
    }
    override def prepareSession(dataSize: DataSize): SparkSession = {
      val sess = SparkSession
        .builder()
        .master("local[4]")
        .appName(name)
        .config(key = "spark.ui.enabled", value = false)
        .getOrCreate()

      source.generate(sess)

      sess
    }

    override def cleanUp(sparkSession: SparkSession): Unit = sparkSession.close()
    override def verify(sparkSession: SparkSession): Unit = {
      import sparkSession.implicits._
      sparkSession.sql(sql).as[Double].collect().toList == List(expectedResult)
    }
    override def requiresVe: Boolean = false
  }

  /** Proof of generating any variation of things */
  val possibilities: List[Testing] = {
    for {
      num <- 2 to 4
      source <- List(Source.CSV, Source.Parquet)
    } yield testSql(
      sql = s"SELECT SUM(value + $num) FROM nums",
      expectedResult = num + 2,
      source = source
    )
  }.toList

}

final class BenchTestingPossibilities extends AnyFreeSpec {

  /** TODO We could also generate Spark plan details from here for easy cross-referencing, as well as codegen */
  BenchTestingPossibilities.possibilities.filterNot(_.requiresVe).foreach { testing =>
    testing.name in {
      val sparkSession = testing.prepareSession(dataSize = DataSize.SanityCheckSize)
      try testing.verify(sparkSession)
      finally testing.cleanUp(sparkSession)
    }
  }
}
