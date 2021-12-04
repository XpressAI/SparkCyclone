package com.nec.spark.planning

import com.nec.arrow.ArrowNativeInterface

import org.apache.spark.sql.VeCachePlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.{CacheTableCommand, ExecutedCommandExec}

class VeColumnarOverrides(
                         arrowNativeInterface: ArrowNativeInterface
                         ) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan match {
    case cache @ ExecutedCommandExec(CacheTableCommand(multipartIdentifier, plan, originalText, isLazy, options)) =>
        VeCachePlan(multipartIdentifier.mkString("."), originalText, arrowNativeInterface)
    case sparkPlan => sparkPlan
  }
}
