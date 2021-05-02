package com.nec.spark.agile


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.SparkPlan
import shapeless._
import shapeless.syntax.sized._
import shapeless.syntax.std.product.productOps

import org.apache.spark.sql.types.{DataType, DoubleType}

object SummingSparkPlanMultipleColumns {

  /** Coalesces all the data into one partition, and then sums it lazily */
  def summingRdd(parentRdd: RDD[ColumnWithNumbers],
                 summer: BigDecimalSummer): RDD[Seq[Double]] =
    parentRdd
      .coalesce(1)
      .mapPartitions(its => {
        val list = its
        val summed = list.map {
          case (index, values) => summer.sum(values.map(BigDecimal(_)).toList).doubleValue()
        }.toList
        Iterator(summed)
      })
}
final case class SummingSparkPlanMultipleColumns(child: SparkPlan,
                                                 attributeMappings: Seq[Seq[AttributeName]],
                                                 summer: BigDecimalSummer) extends SparkPlan {

  /**
   * Extracts the first element, passes through our RDD and then creates another InternalRow RDD in
   * return.
   */
  override protected def doExecute(): RDD[InternalRow] = {
    val columnIndicesMap = child.output.zipWithIndex.map{
      case (attribute, index) => (AttributeName(attribute.name), index)
    }.toMap

    val extractedColumns = attributeMappings
      .map(
        columns => columns.map(attributeName => columnIndicesMap(attributeName))
      )

    lazy val projection = createProjectionForSeq(extractedColumns.size)
    SummingSparkPlanMultipleColumns
      .summingRdd(
        child
          .execute()
          .flatMap(ir => extractRowData(ir, extractedColumns))
          .groupBy(tuple => tuple._1)
          .map {
            case(index, iterable) => (index, iterable.map(_._2))
          },
        summer
      ).map(bd => {
      projection.apply(InternalRow.fromSeq(bd))
    }
    )
  }

  private def extractRowData(row: InternalRow,
                             columns: Seq[Seq[ColumnIndex]]): Seq[(Int, Double)] = {
    columns.zipWithIndex.flatMap {
      case (cols, index) => cols.map(column => ((index, row.getDouble(column))))
    }
  }

  private[agile] def compute(): RDD[InternalRow] = doExecute()

  override def output: Seq[Attribute] = Seq(SingleValueStubPlan.DefaultNumericAttribute)

  override def children: Seq[SparkPlan] = Seq(child)
}
