package com.nec.testing
import com.nec.spark.agile
import com.nec.testing.Testing.DataSize
import com.nec.testing.Testing.TestingTarget
import org.apache.spark.sql.SparkSession

abstract class Testing { this: Product =>
  final def name: agile.CleanName = agile.CleanName.fromString(
    this.getClass.getSimpleName + "_" + this.toString + s"_${testingTarget.label}"
  )
  def verify(sparkSession: SparkSession): Unit
  def benchmark(sparkSession: SparkSession): Unit
  def prepareSession(dataSize: DataSize = DataSize.BenchmarkSize): SparkSession
  def cleanUp(sparkSession: SparkSession): Unit = sparkSession.close()
  def testingTarget: TestingTarget
}

object Testing {

  sealed trait TestingTarget {
    def label: String
    def isPlainSpark: Boolean = this == TestingTarget.PlainSpark
    def isVE: Boolean = this == TestingTarget.VectorEngine
    def isRapids: Boolean = this == TestingTarget.Rapids
    def isC: Boolean = this == TestingTarget.CMake
  }

  object TestingTarget {
    case object Rapids extends TestingTarget {
      def label: String = "Rapids"
    }
    case object VectorEngine extends TestingTarget {
      def label: String = "VE"
    }
    case object CMake extends TestingTarget {
      def label: String = "CMake"
    }
    case object PlainSpark extends TestingTarget {
      def label: String = "JVM"
    }
  }

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
