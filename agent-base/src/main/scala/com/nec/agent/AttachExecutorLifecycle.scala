package com.nec.agent

trait AttachExecutorLifecycle {
  def stopClassName: String
  def startClassName: String
}

object AttachExecutorLifecycle {
  object ServiceBasedExecutorLifecycle extends AttachExecutorLifecycle {
    override def stopClassName: String = classOf[ControlAdviceStop].getCanonicalName
    override def startClassName: String = classOf[ControlAdviceStart].getCanonicalName
  }
}
