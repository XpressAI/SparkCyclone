package com.nec.agent

import net.bytebuddy.agent.builder.AgentBuilder
import net.bytebuddy.description.`type`.TypeDescription
import net.bytebuddy.description.method.MethodDescription
import net.bytebuddy.matcher.ElementMatchers.is
import net.bytebuddy.matcher.ElementMatchers.named
import net.bytebuddy.matcher.ElementMatchers.none

object DriverAttachmentBuilder {
  import net.bytebuddy.pool.TypePool

  val ExtensionPackage = "org.apache.spark.sql"
  val ExtensionClass = "SparkSessionExtensions"

  def using(attachDriverLifecycle: AttachDriverLifecycle): AgentBuilder = {
    val typePool = TypePool.Default.ofSystemLoader

    val tpe = typePool.describe(ExtensionPackage + "." + ExtensionClass).resolve()
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
          .advice(named[MethodDescription]("buildPlannerStrategies"), attachDriverLifecycle.startClassName)
      )
  }
}
