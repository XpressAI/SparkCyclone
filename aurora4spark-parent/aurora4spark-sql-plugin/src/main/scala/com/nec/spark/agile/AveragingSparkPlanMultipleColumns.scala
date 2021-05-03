package com.nec.spark.agile

import com.nec.spark.agile.SingleValueStubPlan.SparkDefaultColumnName

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, UnsafeProjection}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{DataType, DoubleType}

object AveragingSparkPlanMultipleColumns {

  val averageLocalVeo: ColumnWithNumbers => Double = l =>
    BundleExecutor.returningBigDecimalLocal
      .executeBundle(Bundle.avgBigDecimals(l._2.map(double => BigDecimal(double)).toList))
      .toDouble

  val averageRemote: ColumnWithNumbers => Double = l =>
    BundleExecutor.returningBigDecimalRemote
      .executeBundle(Bundle.avgBigDecimals(l._2.map(double => BigDecimal(double)).toList))
      .toDouble

  val averageLocalScala: ColumnWithNumbers => Double = l =>
    l match {
      case (_, numbers) if numbers.nonEmpty => numbers.sum / numbers.size
      case _                                => 0
    }

  /** Coalesces all the data into one partition, and then averages it lazily */
  def averagingRdd(
    parentRdd: RDD[ColumnWithNumbers],
    f: ColumnWithNumbers => Double
  ): RDD[Seq[Double]] =
    parentRdd
      .coalesce(1)
      .mapPartitions(its => {
        Iterator(its.map(columnWithIndex => f(columnWithIndex)).toSeq)
      })

}

final case class AveragingSparkPlanMultipleColumns(
  child: SparkPlan,
  attributeMappings: Seq[Seq[AttributeName]],
  f: ColumnWithNumbers => Double
) extends SparkPlan {

  def computeBD(columnIndices: Seq[Seq[ColumnIndex]]): RDD[Seq[Double]] =
    AveragingSparkPlanMultipleColumns.averagingRdd(
      child
        .execute()
        .flatMap(row => extractRowData(row, columnIndices))
        .groupBy(tuple => tuple._1)
        .map { case (index, iterable) =>
          (index, iterable.map(_._2))
        },
      f
    )

  private def extractRowData(
    row: InternalRow,
    columns: Seq[Seq[ColumnIndex]]
  ): Seq[(Int, Double)] = {
    columns.zipWithIndex.flatMap { case (cols, index) =>
      cols.map(column => ((index, row.getDouble(column))))
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val columnIndicesMap = child.output.zipWithIndex.map { case (attribute, index) =>
      (AttributeName(attribute.name), index)
    }.toMap

    val extractedColumnIndices = attributeMappings
      .map(columns => columns.map(attributeName => columnIndicesMap(attributeName)))

    lazy val projection = createProjectionForSeq(extractedColumnIndices.size)

    computeBD(extractedColumnIndices)
      .map(bd => {
        projection.apply(InternalRow.fromSeq(bd))
      })
  }

  private[agile] def compute(): RDD[InternalRow] = doExecute()

  override def output: Seq[Attribute] = Seq(
    AttributeReference(name = SparkDefaultColumnName, dataType = DoubleType, nullable = false)()
  )

  override def children: Seq[SparkPlan] = Seq(child)
}
