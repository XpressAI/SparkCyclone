package com.nec.ve

import com.nec.colvector.VeColVectorSource
import org.bytedeco.veoffload.global.veo
import org.scalatest.{BeforeAndAfterAll, Suite}

trait WithVeProcess extends BeforeAndAfterAll { this: Suite =>
  implicit def metrics = VeProcessMetrics.noOp
  implicit def source = VeColVectorSource(getClass.getName)
  implicit def veProcess: VeProcess = VeProcess.WrappingVeo(proc_and_ctxt._1, proc_and_ctxt._2, source, VeProcessMetrics.noOp)

  private var initialized = false

  private lazy val proc_and_ctxt = {
    initialized = true
    val selectedVeNodeId = 0
    val proc = veo.veo_proc_create(selectedVeNodeId)
    val ctxt = veo.veo_context_open(proc)

    require(
      proc != null,
      s"Proc could not be allocated for node ${selectedVeNodeId}, got null"
    )
    require(
      ctxt != null,
      s"Async Context could not be allocated for node ${selectedVeNodeId}, got null"
    )

    (proc, ctxt)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    if (initialized) {
      val (proc, thr_ctxt) = proc_and_ctxt
      veo.veo_context_close(thr_ctxt)
      veo.veo_proc_destroy(proc)
    }
  }
}
