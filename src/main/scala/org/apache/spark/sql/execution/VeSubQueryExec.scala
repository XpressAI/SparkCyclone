package org.apache.spark.sql.execution

import com.nec.spark.planning.SupportsVeColBatch
import com.nec.ve.VeColBatch
import org.apache.spark.rdd.RDD
import org.apache.spark.util.ThreadUtils

import scala.concurrent.duration.Duration

case class VeSubQueryExec(name: String, child: SparkPlan)
  extends BaseSubqueryExec
  with UnaryExecNode
  with SupportsVeColBatch {
  override def executeVeColumnar(): RDD[VeColBatch] = sys.error("Should not be called directly")

  def executeCollectVe(): VeColBatch = {
    ThreadUtils.awaitResult(colBatchFuture, Duration.Inf)
  }

  /** Derived from approach of SubQueryExec */
  @transient
  private lazy val colBatchFuture: java.util.concurrent.Future[VeColBatch] = {
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLExecution
      .withThreadLocalCaptured[VeColBatch](sqlContext.sparkSession, SubqueryExec.executionContext) {
        SQLExecution.withExecutionId(sqlContext.sparkSession, executionId) {
          child.asInstanceOf[SupportsVeColBatch].executeVeColumnar().first()
        }
      }
  }
}
