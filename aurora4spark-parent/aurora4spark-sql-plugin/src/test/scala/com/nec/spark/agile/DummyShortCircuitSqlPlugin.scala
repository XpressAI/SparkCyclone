package com.nec.spark.agile

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

/**
 * Dummy plug-in to short-circuit our plan to a specific stubbed value. This helps us establish a
 * base to work off of for future iterations.
 */
object DummyShortCircuitSqlPlugin {
  var applyShortCircuit: Boolean = false
  val StubNumber = BigDecimal(6)
  val StubPlan: SparkPlan = SingleValueStubPlan.forNumber(StubNumber)
}

final class DummyShortCircuitSqlPlugin extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(sparkSessionExtensions: SparkSessionExtensions): Unit = {
    sparkSessionExtensions.injectColumnar({ sparkSession =>
      new ColumnarRule {
        override def preColumnarTransitions: Rule[SparkPlan] =
          sparkPlan => {
            if (DummyShortCircuitSqlPlugin.applyShortCircuit) {
              DummyShortCircuitSqlPlugin.StubPlan
            } else sparkPlan
          }
      }
    })
  }
}
