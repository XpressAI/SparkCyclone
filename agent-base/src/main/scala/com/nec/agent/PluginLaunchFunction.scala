package com.nec.agent

object PluginLaunchFunction {
  import net.bytebuddy.asm.Advice
  @Advice.OnMethodEnter
  def enter(): Unit = {

  }
}
