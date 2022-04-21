package com.nec.ve

import com.nec.colvector.VeColVectorSource
import org.bytedeco.veoffload.global.veo
import org.scalatest.{BeforeAndAfterAll, Suite}

trait WithVeProcess extends BeforeAndAfterAll { this: Suite =>
  implicit def noOpMetrics = VeProcessMetrics.noOp
  implicit def source: VeColVectorSource = VeColVectorSource(s"VE Tests")
  implicit def veProcess: VeProcess = VeProcess.WrappingVeo(proc, source, VeProcessMetrics.noOp)

  private var initialized = false

  private lazy val proc = {
    initialized = true
    veo.veo_proc_create(0)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    if (initialized) {
      veo.veo_proc_destroy(proc)
    }
  }
}
