package com.nec.spark.planning

import com.nec.spark.planning.SupportsVeColBatch.DataCleanup
import com.nec.ve.VeColBatch.VeColVectorSource
import com.nec.ve.{VeColBatch, VeProcess}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.ColumnarBatch

object SupportsVeColBatch {
  sealed trait DataCleanup {
    def cleanup(
      veColBatch: VeColBatch
    )(implicit veProcess: VeProcess, processId: VeColVectorSource): Unit
  }
  object DataCleanup {
    case object Cleanup extends DataCleanup {
      override def cleanup(
        veColBatch: VeColBatch
      )(implicit veProcess: VeProcess, processId: VeColVectorSource): Unit =
        veColBatch.cols.foreach(_.free())
    }

  }
}

trait SupportsVeColBatch { this: SparkPlan =>
  final override def supportsColumnar: Boolean = true

  /**
   * Decide whether the data produced by [[executeVeColumnar()]]
   * should be cleaned up after usage. This is for Spark usage
   */
  def dataCleanup: DataCleanup = DataCleanup.Cleanup
  def executeVeColumnar(): RDD[VeColBatch]
  final override protected def doExecute(): RDD[InternalRow] =
    sys.error("Cannot execute plan without a VE wrapper")
  final override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
    sys.error("Cannot execute plan without a VE wrapper")
}
