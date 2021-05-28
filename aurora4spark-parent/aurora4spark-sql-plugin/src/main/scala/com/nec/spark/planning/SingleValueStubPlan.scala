package com.nec.spark.planning
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.SparkPlan

object SingleValueStubPlan {
  val SparkDefaultColumnName = "value"

  val NumDecimalType = DecimalType.SYSTEM_DEFAULT

  val DefaultNumericAttribute =
    AttributeReference(name = SparkDefaultColumnName, dataType = NumDecimalType, nullable = false)()

  def forNumber(bigDecimal: BigDecimal): SparkPlan = forNumbers(bigDecimal)

  def forNumbers(bigDecimal: BigDecimal*): SparkPlan = {
    LocalTableScanExec(
      Seq(DefaultNumericAttribute),
      bigDecimal.map(v =>
        new GenericInternalRow(Array[Any](org.apache.spark.sql.types.Decimal.apply(v)))
      )
    )
  }
}
