// package com.nec.ve

// import com.nec.spark.SparkCycloneExecutorPlugin
// import com.nec.colvector.VeColVectorSource
// import org.bytedeco.veoffload.global.veo
// import org.apache.spark.SparkConf
// import org.apache.spark.api.plugin.PluginContext
// import org.apache.spark.resource.ResourceInformation
// import com.codahale.metrics.MetricRegistry
// import org.scalatest.{BeforeAndAfterAll, Suite}

// trait WithVeProcess extends BeforeAndAfterAll { this: Suite =>
//   implicit def metrics = VeProcessMetrics.noOp
//   implicit def source = SparkCycloneExecutorPlugin.source
//   implicit def veProcess: VeProcess = VeProcess.WrappingVeo(proc_and_ctxt._1, proc_and_ctxt._2, source, VeProcessMetrics.noOp)

//   private var initialized = false

//   private lazy val proc_and_ctxt = {
//     initialized = true
//     val selectedVeNodeId = 0
//     val proc = veo.veo_proc_create(selectedVeNodeId)
//     val ctxt = veo.veo_context_open(proc)

//     require(
//       proc != null,
//       s"Proc could not be allocated for node ${selectedVeNodeId}, got null"
//     )
//     require(
//       ctxt != null,
//       s"Async Context could not be allocated for node ${selectedVeNodeId}, got null"
//     )

//     (proc, ctxt)
//   }

//   override def beforeAll: Unit = {
//     SparkCycloneExecutorPlugin.pluginContext = new PluginContext {
//       def ask(message: Any): AnyRef = ???
//       def conf: SparkConf = ???
//       def metricRegistry = new MetricRegistry
//       def executorID: String = "executor-id"
//       def hostname: String = "hostname"
//       def resources: java.util.Map[String, ResourceInformation] = ???
//       def send(message: Any): Unit = ???
//     }

//     val (proc, ctx) = proc_and_ctxt
//     SparkCycloneExecutorPlugin._veo_proc = proc
//     SparkCycloneExecutorPlugin._veo_thr_ctxt = ctx
//   }

//   override def afterAll: Unit = {
//     super.afterAll()
//     if (initialized) {
//       val (proc, thr_ctxt) = proc_and_ctxt
//       veo.veo_context_close(thr_ctxt)
//       veo.veo_proc_destroy(proc)
//     }
//   }
// }
