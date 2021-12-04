package org.apache.spark.sql

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import com.nec.arrow.ArrowNativeInterface
import com.nec.spark.planning.{CacheManager, RowToArrowColumnarPlan}
import com.nec.spark.planning.VeColBatchConverters.SparkToVectorEngine

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.ArrowColumnVector

case class VeCachePlan(tableName: String,
                       originalText: Option[String],
                       arrowNativeInterface: ArrowNativeInterface) extends SparkPlan {

  override protected def doExecute(): RDD[InternalRow] = {
    val sparkSession = sqlContext.sparkSession
    val tablePlan = sparkSession.table(tableName).queryExecution.sparkPlan

    val colBatches = SparkToVectorEngine(tablePlan)
      .executeVeColumnar()
      .collect()
      .toList

    CacheManager.cacheTable(colBatches)
    sparkContext.parallelize(Seq.empty)
  }

  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[SparkPlan] = Seq.empty

}
