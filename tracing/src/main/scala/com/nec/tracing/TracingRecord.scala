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
package com.nec.tracing

import com.nec.tracing.TracingRecord.cleanPosition

import java.time.Instant
final case class TracingRecord(
  instant: Instant,
  appName: String,
  appId: String,
  executionId: Instant,
  executorId: Option[String],
  partId: Option[String],
  position: String
) {
  def positionName: String = cleanPosition(position)

  def currentContext: Int = (appName, appId, executionId, partId).hashCode()
}

object TracingRecord {
  def cleanPosition(position: String): String = {
    val s =
      if (position.startsWith("S:") || position.startsWith("E:")) position.drop(2) else position
    s.replaceAll(":L.*", "")
  }
  def parse(str: String): Option[TracingRecord] = {
    PartialFunction
      .condOpt(str.trim.split(" \\$+ ", -1)) { case Array(a, b, c) =>
        PartialFunction.condOpt(b.split("\\|", -1)) {
          case Array(appName, appId, exId, executorId, partId) =>
            TracingRecord(
              instant = Instant.parse(a),
              appName = appName,
              appId = appId,
              executionId = Instant.parse(exId),
              executorId = Some(executorId),
              partId = Some(partId),
              position = c
            )
          case Array(appName, appId, exId, partId) =>
            TracingRecord(
              instant = Instant.parse(a),
              appName = appName,
              appId = appId,
              executionId = Instant.parse(exId),
              executorId = None,
              partId = Some(partId),
              position = c
            )
          case Array(appName, appId, exId) =>
            TracingRecord(
              instant = Instant.parse(a),
              appName = appName,
              appId = appId,
              executionId = Instant.parse(exId),
              executorId = None,
              partId = None,
              position = c
            )
        }
      }
      .flatten
  }
}
