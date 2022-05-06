package com.nec.vectorengine

import com.nec.ve.VeProcessMetrics
import com.nec.colvector.{VeColVectorSource => VeSource}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait WithVeProcess extends BeforeAndAfterAll { self: Suite =>
  implicit def metrics = VeProcessMetrics.noOp
  implicit def source = VeSource(getClass.getName)
  implicit def process: VeProcess = DeferredVeProcess { () =>
    VeProcess.create(getClass.getName)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    process.close
  }
}
