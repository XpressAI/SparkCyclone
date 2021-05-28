package com.nec.spark.planning

import com.nec.spark.agile.BigDecimalSummer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan

object SummingSparkPlan {

  /** Coalesces all the columns into one partition, and then sums it lazily */
  def summingRdd(parentRdd: RDD[Double], summer: BigDecimalSummer): RDD[Double] =
    parentRdd
      .coalesce(1)
      .mapPartitions(its => {
        val results = summer.sum(its.toList.map(BigDecimal(_)))
        Iterator(results.doubleValue())
      })
}

final case class SummingSparkPlan(child: SparkPlan, summer: BigDecimalSummer) extends SparkPlan {

  /**
   * Extracts the first element, passes through our RDD and then creates another InternalRow RDD in
   * return.
   */
  override protected def doExecute(): RDD[InternalRow] = {
    val columns = child.output.size
    SummingSparkPlan
      .summingRdd(
        child
          .execute()
          .flatMap(ir => extractRowData(ir, columns)),
        summer
      )
      .map(bd => {
        ExpressionEncoder[Double]
          .createSerializer()
          .apply(bd)
      })

  }

  private def extractRowData(row: InternalRow, columns: Int): Seq[Double] = {
    Seq
      .range(0, columns)
      .map(index => row.getDouble(index))
  }

  def compute(): RDD[InternalRow] = doExecute()

  override def output: Seq[Attribute] = Seq(SingleValueStubPlan.DefaultNumericAttribute)

  override def children: Seq[SparkPlan] = Seq(child)
}
