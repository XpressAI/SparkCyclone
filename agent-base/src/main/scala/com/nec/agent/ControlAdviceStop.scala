package com.nec.agent
import com.nec.spark.executor.SparkNecExecutorControl

class ControlAdviceStop {}
object ControlAdviceStop {
  import net.bytebuddy.asm.Advice
  @Advice.OnMethodEnter
  def enter(): Unit = { SparkNecExecutorControl.getInstance().stop() }
}
