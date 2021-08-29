package com.nec.agent

import com.nec.agent.DriverExtensionSpec.DriverEvents
import com.nec.spark.driver.TestExtensionControl
import org.apache.spark.sql.SparkSession
import org.scalatest.freespec.AnyFreeSpec

import scala.collection.mutable

object DriverExtensionSpec {
  val DriverEvents = mutable.Buffer.empty[String]
}
final class DriverExtensionSpec extends AnyFreeSpec {
  "Executor is initialized " in {
    import net.bytebuddy.agent.ByteBuddyAgent

    ByteBuddyAgent.install()

    DriverAttachmentBuilder
      .using(classOf[TestExtensionControl].getCanonicalName)
      .installOnByteBuddyAgent()

    val sparkSession = SparkSession.builder().master("local[1]").getOrCreate()
    try {
      import sparkSession.implicits._
      sparkSession.sql("select 1").as[Int].collect().toList
    } finally {
      sparkSession.stop()
    }
    assert(DriverEvents.contains("Intercepted"))
  }
}
