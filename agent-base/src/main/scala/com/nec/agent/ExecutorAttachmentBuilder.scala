package com.nec.agent
import net.bytebuddy.agent.builder.AgentBuilder
import net.bytebuddy.description.`type`.TypeDescription
import net.bytebuddy.description.method.MethodDescription
import net.bytebuddy.matcher.ElementMatchers.is
import net.bytebuddy.matcher.ElementMatchers.named
import net.bytebuddy.matcher.ElementMatchers.none

object ExecutorAttachmentBuilder {

  val ExecutorPackage = "org.apache.spark.executor"
  val ExecutorClass = "Executor"

  def using(attachExecutorLifecycle: AttachExecutorLifecycle): AgentBuilder = {
    import net.bytebuddy.pool.TypePool
    val typePool = TypePool.Default.ofSystemLoader
    val tpe = typePool.describe(ExecutorPackage + "." + ExecutorClass).resolve()
    new AgentBuilder.Default()
      .disableClassFormatChanges()
      .ignore(none[TypeDescription]())
      .`with`(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
      .`with`(AgentBuilder.RedefinitionStrategy.Listener.StreamWriting.toSystemError())
      .`with`(AgentBuilder.Listener.StreamWriting.toSystemError().withTransformationsOnly())
      .`with`(AgentBuilder.InstallationListener.StreamWriting.toSystemError())
      .`type`(is[TypeDescription](tpe))
      .transform(
        new AgentBuilder.Transformer.ForAdvice()
          .advice(named[MethodDescription]("stop"), attachExecutorLifecycle.stopClassName)
      )
      .transform(
        new AgentBuilder.Transformer.ForAdvice()
          .advice(
            named[MethodDescription]("startDriverHeartbeater"),
            attachExecutorLifecycle.startClassName
          )
      )
  }
}
