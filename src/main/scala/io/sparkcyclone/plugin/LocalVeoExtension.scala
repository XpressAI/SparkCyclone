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
package io.sparkcyclone.plugin

import io.sparkcyclone.spark.planning.{VERewriteStrategy, VeRewriteStrategyOptions}
import io.sparkcyclone.sql.rules._
import org.apache.spark.sql.SparkSessionExtensions

final class LocalVeoExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy(sparkSession => {
      new VERewriteStrategy(VeRewriteStrategyOptions.fromConfig(sparkSession.sparkContext.getConf))
    })

    extensions.injectColumnar(_ => CombinedCompilationColumnarRule)
    extensions.injectColumnar(_ => VeColumnarRule)
    extensions.injectPostHocResolutionRule(_ => VeHintResolutionRule)
  }
}
