package com.nec.spark.planning

import com.nec.spark.planning.VeColBatchConverters.SparkToVectorEngine
import com.nec.ve.VeColBatch
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.command.{CacheTableCommand, CreateViewCommand, RunnableCommand}

final case class CacheTableToVeCommand(
  multipartIdentifier: Seq[String],
  originalText: Option[String]
) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // todo add quoting
    val tableName = multipartIdentifier.mkString(".")
    import sparkSession.implicits._
    val veColBatchRDD: List[List[VeColBatch]] = SparkToVectorEngine(
      sparkSession.table(tableName).queryExecution.executedPlan
    ).executeVeColumnar().mapPartitions(it => Iterator.continually(it.toList).take(1)).collect().toList
//    sparkSession.sparkContext.makeRDD(veColBatchRDD).toDF().createTempView()
//    sparkSession.catalog.getTable()
//    CreateViewCommand(
//
//    )
//    veColBatchRDD
//    sparkSession.sessionState.catalog.createTempView(s"${tableName}_ve", InVeMemoryRelation).getCachedTable().cacheTable().dropTempView(tableName)
//    veColBatchRDD.createTempView(tableName)
//    println(s"Cached: ${veColBatchRDD}")
    Seq.empty
  }
}
