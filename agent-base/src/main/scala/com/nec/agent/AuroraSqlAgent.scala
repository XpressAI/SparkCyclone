package com.nec.agent

import java.lang.instrument.Instrumentation

object AuroraSqlAgent {
  def premain(args: String, inst: Instrumentation): Unit = {
    ExecutorAttachmentBuilder
      .using(AttachExecutorLifecycle.ServiceBasedExecutorLifecycle)
      .installOn(inst)

    DriverAttachmentBuilder
      .using(AttachDriverLifecycle.ServiceBasedDriverLifecycle)
      .installOn(inst)
  }
}
