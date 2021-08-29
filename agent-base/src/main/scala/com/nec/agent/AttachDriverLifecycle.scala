package com.nec.agent

import com.nec.spark.driver.SparkNecDriverControl

trait AttachDriverLifecycle {
  def stopClassName: String
  def startClassName: String
}

object AttachDriverLifecycle {
  object ServiceBasedDriverLifecycle extends AttachDriverLifecycle {
    override def stopClassName: String = classOf[ControlAdviceDriverStop].getCanonicalName
    override def startClassName: String = classOf[ControlAdviceDriverStart].getCanonicalName
  }
}

class ControlAdviceDriverStart {}
object ControlAdviceDriverStart {
  import net.bytebuddy.asm.Advice
  @Advice.OnMethodEnter
  def enter(): Unit = { SparkNecDriverControl.getInstance().init() }
}

class ControlAdviceDriverStop {}
object ControlAdviceDriverStop {
  import net.bytebuddy.asm.Advice
  @Advice.OnMethodEnter
  def enter(): Unit = { SparkNecDriverControl.getInstance().stop() }
}
