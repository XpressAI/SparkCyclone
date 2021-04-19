package com.nec.spark.agile

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericInternalRow}
import org.apache.spark.sql.execution.{LocalTableScanExec, SparkPlan}
import org.apache.spark.sql.types.DecimalType

object SingleValueStubPlan {
  private val SparkDefaultColumnName = "value"

  def forNumber(bigDecimal: BigDecimal): SparkPlan = {
    val numDecimalType = DecimalType.SYSTEM_DEFAULT

    LocalTableScanExec(
      Seq(
        AttributeReference(
          name = SparkDefaultColumnName,
          dataType = numDecimalType,
          nullable = false
        )()
      ),
      Seq(new GenericInternalRow(Array[Any](org.apache.spark.sql.types.Decimal.apply(bigDecimal))))
    )
  }
}
