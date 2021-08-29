package com.nec.agent

import net.bytebuddy.agent.builder.AgentBuilder
import net.bytebuddy.description.`type`.TypeDescription
import net.bytebuddy.description.method.MethodDescription
import net.bytebuddy.matcher.ElementMatchers.is
import net.bytebuddy.matcher.ElementMatchers.named
import org.apache.spark.sql.SparkSessionExtensions

object DriverAttachmentBuilder {

  val ExecutorPackage = "org.apache.spark.sql"
  val SSEClass = classOf[SparkSessionExtensions].getSimpleName

  def using(adviceName: String): AgentBuilder = {
    import net.bytebuddy.pool.TypePool
    val typePool = TypePool.Default.ofSystemLoader
    val tpe = typePool.describe(ExecutorPackage + "." + SSEClass).resolve()
    new AgentBuilder.Default()
      .disableClassFormatChanges()
      .`with`(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
      .`with`(AgentBuilder.RedefinitionStrategy.Listener.StreamWriting.toSystemError())
      .`with`(AgentBuilder.Listener.StreamWriting.toSystemError.withTransformationsOnly())
      .`with`(AgentBuilder.InstallationListener.StreamWriting.toSystemError())
      .`type`(is[TypeDescription](tpe))
      .transform(
        new AgentBuilder.Transformer.ForAdvice()
          .advice(named[MethodDescription]("buildPlannerStrategies"), adviceName)
      )
  }
}
