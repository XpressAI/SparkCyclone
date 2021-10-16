package com.nec.tracing

import java.time.Instant
final case class TracingRecord(
  instant: Instant,
  appName: String,
  appId: String,
  executionId: Instant,
  partId: String,
  position: String
) {}

object TracingRecord {
  def parse(str: String): Option[TracingRecord] = {
    PartialFunction
      .condOpt(str.trim.split(" \\$+ ", -1)) { case Array(a, b, c) =>
        PartialFunction.condOpt(b.split("\\|", -1)) { case Array(appName, appId, exId, partId) =>
          TracingRecord(
            instant = Instant.parse(a),
            appName = appName,
            appId = appId,
            executionId = Instant.parse(exId),
            partId = partId,
            position = c
          )
        }
      }
      .flatten
  }
}
