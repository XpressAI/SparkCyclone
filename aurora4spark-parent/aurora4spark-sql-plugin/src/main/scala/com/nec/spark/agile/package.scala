package com.nec.spark

import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{DataType, DoubleType}

package object agile {
  case class AttributeName(value: String) extends AnyVal
  case class SparkPlanWithMetadata(sparkPlan: SparkPlan, attributes: Seq[Seq[AttributeName]])
  type ColumnIndex = Int
  type ColumnWithNumbers = (ColumnIndex, Iterable[Double])

  def createProjectionForSeq(seqSize: Int): UnsafeProjection = {
    val types: Array[DataType] = Seq.fill(seqSize)(DoubleType).toArray
    UnsafeProjection.create(types)
  }
}
