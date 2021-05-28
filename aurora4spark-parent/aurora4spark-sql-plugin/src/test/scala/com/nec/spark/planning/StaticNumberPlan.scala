package com.nec.spark.planning
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.rdd.RDD

final case class StaticNumberPlan(n: BigDecimal) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    rdd
  }

  override def output: Seq[Attribute] = Seq(SingleValueStubPlan.DefaultNumericAttribute)

  @transient private lazy val unsafeRows: Array[InternalRow] = {
    Array(
      ExpressionEncoder[BigDecimal]
        .createSerializer()
        .apply(n)
    )
  }

  @transient private lazy val rdd: RDD[InternalRow] = {
    val numSlices = math.min(unsafeRows.length, sqlContext.sparkContext.defaultParallelism)
    sqlContext.sparkContext.parallelize(unsafeRows, numSlices)
  }
}
