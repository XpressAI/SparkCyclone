package com.nec.tracing

import java.time.Instant

final case class TracingRecord(instant: Instant, recordId: String, position: String) {}

object TracingRecord {
  def parse(str: String): Option[TracingRecord] = {
    PartialFunction.condOpt(str.trim.split(" \\$+ ", -1)) { case Array(a, b, c) =>
      TracingRecord(instant = Instant.parse(a), recordId = b, position = c)
    }
  }
}
