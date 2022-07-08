package io.sparkcyclone.benchmarks

import io.sparkcyclone.data.vector._
import io.sparkcyclone.data.VeColVectorSource
import io.sparkcyclone.vectorengine._
import scala.collection.mutable.{ArrayBuffer => MBuf}
import org.openjdk.jmh.annotations._

object CompressedBatchVeTransferBenchmarks {
  implicit val source = VeColVectorSource(getClass.getName)

  @State(Scope.Benchmark)
  class Fixture {
    // Benchmark results are grouped by lexicographical order of the parameters
    @Param(Array("100", "1000", "10000", "100000", "1000000",  "10000000", "100000000"))
    var size: Int = _

    @Param(Array("Short", "Int", "Double", "String"))
    var typ: String = _

    @Param(Array("10"))
    var ncolumns: Int = _

    // Setup
    implicit var process: VeProcess = _
    var input: CompressedBytePointerColBatch = _

    // Temporary data
    val data = MBuf.empty[CompressedVeColBatch]

    @Setup(Level.Trial)
    def setup0: Unit = {
      process = DeferredVeProcess { () =>
        VeProcess.create(getClass.getName, 4)
      }

      // Initialize CompressedBytePointerColBatch
      input = BytePointerColBatch(InputSamples.bpcv(typ, ncolumns, size)).compressed
    }

    @TearDown(Level.Invocation)
    def teardown2: Unit = {
      data.map(_.free)
      data.clear
    }

    @TearDown(Level.Trial)
    def teardown0: Unit = {
      process.close
    }
  }
}

class CompressedBatchVeTransferBenchmarks {
  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 5)
  @Measurement(iterations = 10)
  @BenchmarkMode(Array(Mode.AverageTime))
  def benchmark1(fixture: CompressedBatchVeTransferBenchmarks.Fixture): Int = {
    implicit val process = fixture.process

    fixture.data += fixture.input.toCompressedVeColBatch
    fixture.input.columns.size
  }
}
