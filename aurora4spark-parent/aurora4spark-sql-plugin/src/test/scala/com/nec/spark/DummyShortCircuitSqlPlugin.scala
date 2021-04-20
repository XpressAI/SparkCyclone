package com.nec.spark

import com.nec.spark.DummyShortCircuitSqlPlugin.{applyShortCircuit, StubPlan}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericInternalRow}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, LocalTableScanExec, SparkPlan}
import org.apache.spark.sql.types.DecimalType

/**
 * Dummy plug-in to short-circuit our plan to a specific stubbed value. This helps us establish a
 * base to work off of for future iterations.
 */
object DummyShortCircuitSqlPlugin {
  var applyShortCircuit: Boolean = false
  val StubPlan: SparkPlan = {

    val StubNumber = BigDecimal(6)
    val numDecimalType =
      DecimalType(StubNumber.precision, StubNumber.scale)
    val SparkDefaultColumnName = "value"

    LocalTableScanExec(
      Seq(
        AttributeReference(
          name = SparkDefaultColumnName,
          dataType = numDecimalType,
          nullable = false
        )()
      ),
      Seq(new GenericInternalRow(Array[Any](org.apache.spark.sql.types.Decimal.apply(StubNumber))))
    )
  }
}

final class DummyShortCircuitSqlPlugin extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(sparkSessionExtensions: SparkSessionExtensions): Unit = {

    sparkSessionExtensions.injectColumnar({ sparkSession =>
      new ColumnarRule {
        override def preColumnarTransitions: Rule[SparkPlan] =
          sparkPlan => {
            if (applyShortCircuit) StubPlan
            else sparkPlan
          }
      }
    })
  }
}
