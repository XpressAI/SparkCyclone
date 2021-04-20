package com.nec.spark.agile

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.Decimal

object SummingSparkPlan {

  /** Coalesces all the data into one partition, and then sums it lazily */
  def summingRdd(parentRdd: RDD[BigDecimal], summer: BigDecimalSummer): RDD[BigDecimal] =
    parentRdd
      .coalesce(1)
      .mapPartitions(its => {
        Iterator(summer.sum(its.toList))
      })
}

final case class SummingSparkPlan(child: SparkPlan, summer: BigDecimalSummer) extends SparkPlan {

  /**
   * Extracts the first element, passes through our RDD and then creates another InternalRow RDD in
   * return.
   */
  override protected def doExecute(): RDD[InternalRow] =
    SummingSparkPlan
      .summingRdd(
        child
          .execute()
          .map { ir =>
            ir.get(0, child.output.head.dataType).asInstanceOf[Decimal].toBigDecimal
          },
        summer
      )
      .map(bd => ExpressionEncoder[BigDecimal].createSerializer().apply(bd))

  private[agile] def compute(): RDD[InternalRow] = doExecute()

  override def output: Seq[Attribute] = Seq(SingleValueStubPlan.DefaultNumericAttribute)

  override def children: Seq[SparkPlan] = Seq(child)
}
