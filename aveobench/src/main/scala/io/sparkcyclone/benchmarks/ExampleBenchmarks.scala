package io.sparkcyclone.benchmarks

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

object ExampleBenchmarks {
  @State(Scope.Benchmark)
  class Params {
    // Benchmark results are grouped by lexicographical order of the parameters
    @Param(Array("100", "1000", "10000"))
    var size: Int = _

    @Param(Array("1", "2"))
    var factor: Int = _
  }
}

class ExampleBenchmarks {
  @Benchmark
  @Fork(value = 1)
  @BenchmarkMode(Array(Mode.AverageTime))
  def benchmark1(params: ExampleBenchmarks.Params, sink: Blackhole): Double = {
    val sum: Double = List.range(1, params.size * params.factor).sum
    sink.consume(sum)
    sum
  }
}
