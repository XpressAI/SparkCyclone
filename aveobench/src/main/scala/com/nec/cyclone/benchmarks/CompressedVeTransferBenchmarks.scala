// package com.nec.cyclone.benchmarks

// import com.nec.cyclone.colvector.BytePointerColVectorOps._
// import com.nec.cyclone.colvector.CompressedBytePointerColVector
// import com.nec.colvector._
// import com.nec.colvector.SeqOptTConversions._
// import com.nec.util.CallContextOps._
// import com.nec.ve._
// import org.bytedeco.veoffload.global.veo
// import scala.reflect._
// import scala.collection.mutable.{ArrayBuffer => MBuf}
// import org.bytedeco.veoffload.{veo_proc_handle, veo_thr_ctxt}
// import org.openjdk.jmh.annotations._
// import org.openjdk.jmh.infra.Blackhole

// object CompressedVeTransferBenchmarks {
//   implicit val source = VeColVectorSource(getClass.getName)

//   @State(Scope.Benchmark)
//   class Fixture {
//     // Benchmark results are grouped by lexicographical order of the parameters
//     @Param(Array("100", "1000", "10000", "100000", "1000000",  "10000000", "100000000"))
//     var size: Int = _

//     @Param(Array("Short", "Int", "Double", "String"))
//     var typ: String = _

//     @Param(Array("10"))
//     var ncolumns: Int = _

//     // Setup
//     var handle: veo_proc_handle = _
//     var process: VeProcess.WrappingVeo = _
//     var tcontext: veo_thr_ctxt = _
//     var inputs: Seq[CompressedBytePointerColVector] = _

//     // Temporary data
//     val data = MBuf.empty[VeColVector]

//     @Setup(Level.Trial)
//     def setup0: Unit = {
//       handle = veo.veo_proc_create(0)
//       tcontext = veo.veo_context_open(handle)
//       process = VeProcess.WrappingVeo(handle, tcontext, source, VeProcessMetrics.noOp)

//       // Initialize CompressedBytePointerColVector
//       inputs = InputSamples.bpcv(typ, ncolumns, size).map(_.compressed)
//     }

//     @TearDown(Level.Invocation)
//     def teardown2: Unit = {
//       implicit val p = process
//       data.map(_.free)
//       data.clear
//     }

//     @TearDown(Level.Trial)
//     def teardown0: Unit = {
//       Option(handle).map(veo.veo_proc_destroy)
//     }
//   }
// }

// class MultipleCompressedVeTransferBenchmarks {
//   @Benchmark
//   @Fork(value = 1)
//   @Warmup(iterations = 2)
//   @Measurement(iterations = 5)
//   @BenchmarkMode(Array(Mode.AverageTime))
//   def benchmark1(fixture: CompressedVeTransferBenchmarks.Fixture): Int = {
//     implicit val process = fixture.process
//     implicit val source = process.source
//     implicit val metrics = process.veProcessMetrics

//     fixture.data ++= fixture.inputs.map(_.toVeColVector)
//     fixture.inputs.size
//   }

//   @Benchmark
//   @Fork(value = 1)
//   @Warmup(iterations = 2)
//   @Measurement(iterations = 5)
//   @BenchmarkMode(Array(Mode.AverageTime))
//   def benchmark2(fixture: CompressedVeTransferBenchmarks.Fixture): Int = {
//     implicit val process = fixture.process
//     implicit val source = process.source
//     implicit val metrics = process.veProcessMetrics

//     // Allocate
//     val resultFs = fixture.inputs.map(_.asyncToVeColVector)
//     // Transfer asynchronously and wait
//     fixture.data ++= resultFs.map(_.apply()).map(_.get())
//     fixture.inputs.size
//   }
// }
