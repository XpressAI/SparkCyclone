package com.nec.spark.agile

import com.nec.spark.agile.SingleValueStubPlan.SparkDefaultColumnName
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.DoubleType

object AveragingSparkPlan {

  val averageLocalVeo: List[Double] => Double = l =>
    BundleExecutor.returningBigDecimalLocal
      .executeBundle(Bundle.avgBigDecimals(l.map(double => BigDecimal(double))))
      .toDouble

  val averageRemote: List[Double] => Double = l =>
    BundleExecutor.returningBigDecimalRemote
      .executeBundle(Bundle.avgBigDecimals(l.map(double => BigDecimal(double))))
      .toDouble

  val averageLocalScala: List[Double] => Double = l => if (l.nonEmpty) l.sum / l.size else 0

  /** Coalesces all the columns into one partition, and then averages it lazily */
  def averagingRdd(parentRdd: RDD[Double], f: List[Double] => Double): RDD[Double] =
    parentRdd
      .coalesce(1)
      .mapPartitions(its => {
        Iterator(f(its.toList))
      })
}

final case class AveragingSparkPlan(child: SparkPlan, f: List[Double] => Double) extends SparkPlan {

  def computeBD(): RDD[Double] = AveragingSparkPlan
    .averagingRdd(
      child
        .execute()
        .map(_.getDouble(0)),
      f
    )

  /**
   * Extracts the first element, passes through our RDD and then creates another InternalRow RDD in
   * return.
   */
  override protected def doExecute(): RDD[InternalRow] =
    computeBD()
      .map(bd =>
        ExpressionEncoder[Double]
          .createSerializer()
          .apply(bd)
      )

  private[agile] def compute(): RDD[InternalRow] = doExecute()

  override def output: Seq[Attribute] = Seq(
    AttributeReference(name = SparkDefaultColumnName, dataType = DoubleType, nullable = false)()
  )

  override def children: Seq[SparkPlan] = Seq(child)
}
