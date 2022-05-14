package com.nec.cyclone.benchmarks

import com.nec.colvector._
import com.nec.colvector.SeqOptTConversions._
import com.nec.util.CallContextOps._
import com.nec.ve._
import org.bytedeco.veoffload.global.veo
import org.bytedeco.veoffload.{veo_proc_handle, veo_thr_ctxt}
import org.openjdk.jmh.annotations._

object VeFreeBenchmarks {
  implicit val source = VeColVectorSource(getClass.getName)

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
    var handle: veo_proc_handle = _
    var process: VeProcess.WrappingVeo = _
    var tcontext: veo_thr_ctxt = _
    var inputs: Seq[VeColVector] = _

    @Setup(Level.Trial)
    def setup0: Unit = {
      handle = veo.veo_proc_create(0)
      tcontext = veo.veo_context_open(handle)
      process = VeProcess.WrappingVeo(handle, tcontext, source, VeProcessMetrics.noOp)

      // Initialize BytePointerColVectors
      val bpcvs = InputSamples.bpcv(typ, ncolumns, size)

      implicit val p = process
      implicit val m = process.veProcessMetrics

      // Transfer the data to the VE first, since the benchmark will measure the free()
      inputs = bpcvs.map(_.asyncToVeColVector)
        .map(_.apply())
        .map(_.get())
    }

    @TearDown(Level.Trial)
    def teardown0: Unit = {
      Option(handle).map(veo.veo_proc_destroy)
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
    implicit val source = process.source
    implicit val metrics = process.veProcessMetrics

    fixture.inputs.map(_.free)
    fixture.inputs.size
  }
}
