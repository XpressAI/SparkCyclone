/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.nec.spark

import com.nec.spark.LocalVeoExtension.compilerRule
import com.nec.spark.planning.hints._
import com.nec.spark.planning.{CombinedCompilationColumnarRule, VERewriteStrategy, VeColumnarRule, VeRewriteStrategyOptions}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnresolvedHint}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.ColumnarRule
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

object LocalVeoExtension extends LazyLogging {
  var _enabled = true

//  def compilerRule(sparkSession: SparkSession): ColumnarRule = ParallelCompilationColumnarRule
  def compilerRule(sparkSession: SparkSession): ColumnarRule = CombinedCompilationColumnarRule
}

final class LocalVeoExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(sparkSessionExtensions: SparkSessionExtensions): Unit = {

    sparkSessionExtensions.injectPlannerStrategy(sparkSession => {
      new VERewriteStrategy(
        options = VeRewriteStrategyOptions.fromConfig(sparkSession.sparkContext.getConf)
      )
    })
    sparkSessionExtensions.injectColumnar(compilerRule)
    sparkSessionExtensions.injectColumnar(_ => new VeColumnarRule)

    class MyRule extends Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = {
        plan.resolveOperatorsUp { case (h: UnresolvedHint) =>
          (h.name, h.parameters) match {
            case ("SORT_ON_VE", Seq(Literal(bool: Boolean, BooleanType))) => SortOnVe(h.child, bool)
            case ("PROJECT_ON_VE", Seq(Literal(bool: Boolean, BooleanType))) => ProjectOnVe(h.child, bool)
            case ("FILTER_ON_VE", Seq(Literal(bool: Boolean, BooleanType)))=> FilterOnVe(h.child, bool)
            case ("AGGREGATE_ON_VE", Seq(Literal(bool: Boolean, BooleanType)))=> AggregateOnVe(h.child, bool)
            case ("EXCHANGE_ON_VE", Seq(Literal(bool: Boolean, BooleanType)))=> ExchangeOnVe(h.child, bool)
            case ("FAIL_FAST", Seq(Literal(bool: Boolean, BooleanType)))=> FailFast(h.child, bool)
            case ("JOIN_ON_VE", Seq(Literal(bool: Boolean, BooleanType)))=> JoinOnVe(h.child, bool)
            case ("AMPLIFY_BATCHES", Seq(Literal(bool: Boolean, BooleanType)))=> AmplifyBatches(h.child, bool)
            case ("SKIP_VE", Seq(Literal(bool: Boolean, BooleanType)))=> SkipVe(h.child, bool)
          }
        }
      }
    }

    sparkSessionExtensions.injectPostHocResolutionRule(sparkSession => {
      new MyRule()
    })
  }
}
