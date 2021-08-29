package com.nec.spark.driver

import com.nec.agent.DriverExtensionSpec.DriverEvents
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

final class TestExtensionControl() {}

object TestExtensionControl {
  import net.bytebuddy.asm.Advice
  @Advice.OnMethodEnter
  def enter(@Advice.This obj: Object): Unit = {
    obj
      .asInstanceOf[SparkSessionExtensions]
      .injectPlannerStrategy(_ => {
        new Strategy {
          override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
            DriverEvents.append("Intercepted")
            Seq.empty
          }
        }
      })
  }
}
