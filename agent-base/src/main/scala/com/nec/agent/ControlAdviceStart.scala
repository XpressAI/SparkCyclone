package com.nec.agent
import com.nec.spark.executor.SparkNecExecutorControl

class ControlAdviceStart {}
object ControlAdviceStart {
  import net.bytebuddy.asm.Advice
  @Advice.OnMethodEnter
  def enter(): Unit = { SparkNecExecutorControl.getInstance().init() }
}
