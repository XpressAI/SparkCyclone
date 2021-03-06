package io.sparkcyclone.spark.plans

import io.sparkcyclone.data.vector.VeColBatch
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.ColumnarBatch

object SupportsVeColBatch extends LazyLogging {}

trait SupportsVeColBatch { this: SparkPlan =>
  /*
    Set `supportsColumnar` to be true to annotate plans as supporting columnar
    executionp (even though we define the custom `executeVeColumnar()` to support
    outputting `RDD[VeColBatch]`.
  */
  final override def supportsColumnar: Boolean = true

  /**
   * Decide whether the data produced by [[executeVeColumnar()]]
   * should be cleaned up after usage. This is for Spark usage
   */
  def dataCleanup: DataCleanup = DataCleanup.cleanup(this.getClass)

  /**
    VE-based plans are neither executed with `execute()` nor `executeColumnar()`
    since they only support returning `RDD[InternalRow]` and `RDD[ColumnarBatch]`,
    respectively.
  */
  def executeVeColumnar(): RDD[VeColBatch]

  final override protected def doExecute(): RDD[InternalRow] = {
    sys.error("Cannot execute plan without a VE wrapper")
  }

  final override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    sys.error("Cannot execute plan without a VE wrapper")
  }
}
