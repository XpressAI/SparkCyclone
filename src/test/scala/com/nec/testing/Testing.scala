package com.nec.testing
import com.nec.spark.agile
import com.nec.spark.agile.CleanName
import com.nec.testing.Testing.DataSize
import com.nec.testing.Testing.TestingTarget
import org.apache.spark.sql.SparkSession

abstract class Testing { this: Product =>
  final def name: CleanName = agile.CleanName.fromString(
    this.getClass.getSimpleName + "__" + this.toString + s"_${testingTarget.label}"
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
    def isCMake: Boolean = this == TestingTarget.CMake
    def expectedString: Option[String]
  }

  object TestingTarget {
    case object Rapids extends TestingTarget {
      def label: String = "Rapids"
      override def expectedString: Option[String] = Some("Gpu")
    }
    case object VectorEngine extends TestingTarget {
      def label: String = "VE"
      override def expectedString: Option[String] = Some("LocalVeoNativeEvaluator")
    }
    case object CMake extends TestingTarget {
      def label: String = "CMake"
      override def expectedString: Option[String] = Some("CEvaluation")
    }
    case object PlainSpark extends TestingTarget {
      def label: String = "JVM"
      override def expectedString: Option[String] = None
    }
  }

  /**
   * We may prepare a session with a small amount of data, but also with a big amount of data
   *
   * This enables us to confirm the *correctness* before we proceed with heavy benchmarking.
   */
  sealed trait DataSize {
    def label: String
  }
  object DataSize {
    case object BenchmarkSize extends DataSize {
      override def label: String = "Large"
    }
    case object SanityCheckSize extends DataSize {
      override def label: String = "Small"
    }
  }
}
