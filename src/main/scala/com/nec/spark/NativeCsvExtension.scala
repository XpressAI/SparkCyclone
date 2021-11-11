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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import com.nec.spark.planning.NativeCsvExec
import com.nec.native.NativeEvaluator.CNativeEvaluator
import com.nec.native.NativeEvaluator.ExecutorPluginManagedEvaluator

final class NativeCsvExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(sparkSessionExtensions: SparkSessionExtensions): Unit = {
    sparkSessionExtensions.injectPlannerStrategy { sparkSession =>
      val conf = sparkSession.sparkContext.getConf
      val selection = conf.get("spark.com.nec.native-csv", "false").toLowerCase()
      selection match {
        case "x86" => NativeCsvExec.NativeCsvStrategy(CNativeEvaluator)
        case "ve"  => NativeCsvExec.NativeCsvStrategy(ExecutorPluginManagedEvaluator)
        case _     => EmptyStrategy
      }
    }
  }
}
