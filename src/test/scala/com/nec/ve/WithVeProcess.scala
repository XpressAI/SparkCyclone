package com.nec.ve

import com.nec.colvector.VeColVectorSource
import org.bytedeco.veoffload.global.veo
import org.scalatest.{BeforeAndAfterAll, Suite}

trait WithVeProcess extends BeforeAndAfterAll { this: Suite =>
  implicit def metrics = VeProcessMetrics.noOp
  implicit def source = VeColVectorSource(getClass.getName)
  implicit def veProcess: VeProcess = VeProcess.WrappingVeo(proc, thr_ctxt, source, VeProcessMetrics.noOp)

  private var initialized = false

  private lazy val proc = {
    initialized = true
    veo.veo_proc_create(0)
  }

  private lazy val thr_ctxt = {
    veo.veo_context_open(proc)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    if (initialized) {
      veo.veo_context_close(thr_ctxt);
      veo.veo_proc_destroy(proc)
    }
  }
}
