package io.sparkcyclone.benchmarks

import org.scalatest.freespec.AnyFreeSpec

final class ExtractVeMetricsSpec extends AnyFreeSpec {
  "Get the metric" in {
    val message =
      "type=GAUGE, name=application_1638487109505_0594.2.VEProcessExecutor.ve.allocations, value=3621"
    assert(MetricCapture.matches(message))
  }
  "Something else does not match" in {
    assert(!MetricCapture.matches("test"))
  }
}
