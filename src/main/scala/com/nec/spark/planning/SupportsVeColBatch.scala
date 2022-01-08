package com.nec.spark.planning

import com.nec.spark.SparkCycloneExecutorPlugin.cleanUpIfNotCached
import com.nec.spark.planning.SupportsVeColBatch.DataCleanup
import com.nec.ve.VeColBatch.VeColVectorSource
import com.nec.ve.{VeColBatch, VeProcess}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.ColumnarBatch

object SupportsVeColBatch extends LazyLogging {
  trait DataCleanup {
    def cleanup(veColBatch: VeColBatch)(implicit
      veProcess: VeProcess,
      processId: VeColVectorSource,
      fullName: sourcecode.FullName,
      line: sourcecode.Line
    ): Unit
  }
  object DataCleanup {
    def noCleanup(parent: Class[_]): DataCleanup = new DataCleanup {
      override def cleanup(veColBatch: VeColBatch)(implicit
        veProcess: VeProcess,
        processId: VeColVectorSource,
        fullName: sourcecode.FullName,
        line: sourcecode.Line
      ): Unit = logger.debug(
        s"Not cleaning up data at ${processId} / ${veColBatch.underlying.cols
          .map(_.containerLocation)} - from ${fullName.value}#${line.value}, directed by ${parent.getCanonicalName}"
      )
    }
    def cleanup(parent: Class[_]): DataCleanup = new DataCleanup {
      override def cleanup(veColBatch: VeColBatch)(implicit
        veProcess: VeProcess,
        processId: VeColVectorSource,
        fullName: sourcecode.FullName,
        line: sourcecode.Line
      ): Unit = {
        logger.debug(
          s"Requesting to clean up data at ${processId} by ${fullName.value}#${line.value}, directed by ${parent.getCanonicalName}"
        )
        cleanUpIfNotCached(veColBatch)(fullName, line)
      }
    }

  }
}

trait SupportsVeColBatch { this: SparkPlan =>
  final override def supportsColumnar: Boolean = true

  /**
   * Decide whether the data produced by [[executeVeColumnar()]]
   * should be cleaned up after usage. This is for Spark usage
   */
  def dataCleanup: DataCleanup = DataCleanup.cleanup(this.getClass)

  def executeVeColumnar(): RDD[VeColBatch]
  final override protected def doExecute(): RDD[InternalRow] =
    sys.error("Cannot execute plan without a VE wrapper")
  final override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
    sys.error("Cannot execute plan without a VE wrapper")
}
