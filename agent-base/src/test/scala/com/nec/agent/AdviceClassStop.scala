package com.nec.agent
import com.nec.agent.ExecutorExtensionSpec.RecordedEvents

final class AdviceClassStop() {

}
object AdviceClassStop {
  import net.bytebuddy.asm.Advice
  @Advice.OnMethodEnter
  def enter(): Unit = { RecordedEvents.append("Closed") }
}
