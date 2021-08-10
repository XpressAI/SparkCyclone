package com.nec.agent

import com.nec.agent.ExecutorExtensionSpec.RecordedEvents
import org.apache.spark.sql.SparkSession
import org.scalatest.freespec.AnyFreeSpec

object ExecutorExtensionSpec {
  val RecordedEvents = scala.collection.mutable.Buffer.empty[String]
}

final class ExecutorExtensionSpec extends AnyFreeSpec {
  "Executor is initialized " in {
    import net.bytebuddy.agent.ByteBuddyAgent

    ByteBuddyAgent.install()

    ExecutorAttachmentBuilder
      .using(new AttachExecutorLifecycle {
        override def stopClassName: String = classOf[AdviceClassStop].getCanonicalName
        override def startClassName: String = classOf[AdviceClassStart].getCanonicalName
      })
      .installOnByteBuddyAgent()

    RecordedEvents.append("Preparing")
    val sparkSession = SparkSession.builder().master("local[1]").getOrCreate()
    try {
      import sparkSession.implicits._
      sparkSession.sql("select 1").as[Int].collect().toList
      RecordedEvents.append("Computed")
    } finally {
      RecordedEvents.append("Closing")
      sparkSession.stop()
    }
    assert(
      RecordedEvents.toList == List("Preparing", "Initialized", "Computed", "Closing", "Closed")
    )
  }
}
