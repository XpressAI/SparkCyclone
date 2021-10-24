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
