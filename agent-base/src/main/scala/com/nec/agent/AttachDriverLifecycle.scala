package com.nec.agent

import com.nec.spark.driver.SparkNecDriverControl
import com.carrotsearch.hppc.HashOrderMixing.Strategy
import org.apache.spark.sql.execution.SparkPlan

trait AttachDriverLifecycle {
  def startClassName: String
}

object AttachDriverLifecycle {
  object ServiceBasedDriverLifecycle extends AttachDriverLifecycle {
    override def startClassName: String = classOf[ControlAdviceDriverStart].getCanonicalName
  }
}

class ControlAdviceDriverStart {}
object ControlAdviceDriverStart {
  import net.bytebuddy.asm.Advice
  @Advice.OnMethodEnter
  def enter(@Advice.This obj: Object): Unit = { 
      SparkNecDriverControl.getInstance().init(obj)
  }
}
