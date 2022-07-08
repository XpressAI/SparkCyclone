package io.sparkcyclone.benchmarks

import io.sparkcyclone.data.vector.BytePointerColVectorOps._
import io.sparkcyclone.data.vector.CompressedBytePointerColVector
import io.sparkcyclone.data.vector._
import io.sparkcyclone.vectorengine._
import scala.collection.mutable.{ArrayBuffer => MBuf}
import org.openjdk.jmh.annotations._

object CompressedVeTransferBenchmarks {
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
    var inputs: Seq[CompressedBytePointerColVector] = _

    // Temporary data
    val data = MBuf.empty[VeColVector]

    @Setup(Level.Trial)
    def setup0: Unit = {
      process = DeferredVeProcess { () =>
        VeProcess.create(getClass.getName, 4)
      }
      implicit val source = process.source

      // Initialize CompressedBytePointerColVector
      inputs = InputSamples.bpcv(typ, ncolumns, size).map(_.compressed)
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

class MultipleCompressedVeTransferBenchmarks {
  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Array(Mode.AverageTime))
  def benchmark1(fixture: CompressedVeTransferBenchmarks.Fixture): Int = {
    implicit val process = fixture.process
    implicit val source = process.source

    fixture.data ++= fixture.inputs.map(_.toVeColVector)
    fixture.inputs.size
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Array(Mode.AverageTime))
  def benchmark2(fixture: CompressedVeTransferBenchmarks.Fixture): Int = {
    implicit val process = fixture.process

    // Allocate
    val resultFs = fixture.inputs.map(_.asyncToVeColVector)

    // Transfer asynchronously and wait
    fixture.data ++= resultFs.map(_.apply()).map(_.get)
    fixture.inputs.size
  }
}
