package com.nec.agent
import com.nec.agent.ExecutorExtensionSpec.RecordedEvents

final class AdviceClassStart() {

}
object AdviceClassStart {
  import net.bytebuddy.asm.Advice
  @Advice.OnMethodEnter
  def enter(): Unit = { RecordedEvents.append("Initialized") }
}
