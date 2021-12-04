package com.nec.spark.planning

import com.nec.spark.planning.VeColBatchConverters.{SparkToVectorEngine, VectorEngineToSpark}

import org.apache.spark.sql.VeCachePlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.{CacheTableCommand, ExecutedCommandExec}
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

class VeColumnarRule extends ColumnarRule {
  override def preColumnarTransitions: Rule[SparkPlan] = {
    case cache @ ExecutedCommandExec(
          CacheTableCommand(multipartIdentifier, plan, originalText, isLazy, options)
        ) =>
      println("Here")
      VeCachePlan(multipartIdentifier.mkString("."), originalText)
    case plan =>
      plan.transform {
        case RowToArrowColumnarPlan(ArrowColumnarToRowPlan(child)) => child
        case SparkToVectorEngine(VectorEngineToSpark(child))       => child
      }
  }

}
