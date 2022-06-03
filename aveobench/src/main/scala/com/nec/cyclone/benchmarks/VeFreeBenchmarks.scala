package com.nec.cyclone.benchmarks

import com.nec.colvector._
import com.nec.vectorengine._
import org.openjdk.jmh.annotations._

object VeFreeBenchmarks {
  @State(Scope.Benchmark)
  class Fixture {
    // Benchmark results are grouped by lexicographical order of the parameters
    @Param(Array("100", "1000", "10000", "100000"))
    var size: Int = _

    @Param(Array("Short", "Int", "Double", "String"))
    var typ: String = _

    @Param(Array("10", "100", "1000"))
    var ncolumns: Int = _

    // Setup
    implicit var process: VeProcess = _
    var inputs: Seq[VeColVector] = _

    @Setup(Level.Trial)
    def setup0: Unit = {
      process = DeferredVeProcess { () =>
        VeProcess.create(getClass.getName)
      }
      implicit val source = process.source

      // Initialize BytePointerColVectors
      val bpcvs = InputSamples.bpcv(typ, ncolumns, size)

      // Transfer the data to the VE first, since the benchmark will measure the free()
      inputs = bpcvs.map(_.asyncToVeColVector).map(_.apply()).map(_.get)
    }

    @TearDown(Level.Trial)
    def teardown0: Unit = {
      process.close
    }
  }
}

class VeFreeBenchmarks {
  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Array(Mode.AverageTime))
  def benchmark1(fixture: VeFreeBenchmarks.Fixture): Int = {
    implicit val process = fixture.process

    fixture.inputs.map(_.free)
    fixture.inputs.size
  }
}
