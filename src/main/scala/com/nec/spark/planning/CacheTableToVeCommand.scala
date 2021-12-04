package com.nec.spark.planning

import com.nec.spark.planning.VeColBatchConverters.SparkToVectorEngine
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.command.{CacheTableCommand, RunnableCommand}

final case class CacheTableToVeCommand(
  multipartIdentifier: Seq[String],
  originalText: Option[String]
) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    // todo add quoting
    val tableName = multipartIdentifier.mkString(".")
    import sparkSession.implicits._
    val veColBatchRDD: DataFrame = SparkToVectorEngine(
      sparkSession.table(tableName).queryExecution.executedPlan
    ).executeVeColumnar().toDF()
    sparkSession.sharedState.cacheManager.cacheQuery(veColBatchRDD, Some(tableName))
    println(s"Cached: ${veColBatchRDD}")
    Seq.empty
  }
}
