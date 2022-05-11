package com.nec.vectorengine

import com.nec.ve.VeProcessMetrics
import com.nec.colvector.{VeColVectorSource => VeSource}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait WithVeProcess extends BeforeAndAfterAll { self: Suite =>
  implicit val metrics = VeProcessMetrics.noOp

  /*
    Initialization is explicitly deferred to avoid creation of VeProcess when
    running tests in non-VE scope, because the the instantiation of ScalaTest
    classes is eager even if annotated with @VectorEngineTest
  */
  implicit val process: VeProcess = DeferredVeProcess { () =>
    VeProcess.create(getClass.getName)
  }

  implicit def source: VeSource = {
    process.source
  }

  implicit var engine: VectorEngine = _

  override def beforeAll: Unit = {
    super.beforeAll
    engine = new VectorEngineImpl(process, new VectorEngineMetrics {})
  }

  override def afterAll: Unit = {
    // Free all memory held by the process and close
    process.freeAll
    process.close
    super.afterAll
  }
}
