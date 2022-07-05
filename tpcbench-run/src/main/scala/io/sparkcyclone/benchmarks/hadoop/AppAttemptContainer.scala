package io.sparkcyclone.benchmarks.hadoop

import scala.xml.Elem

final case class AppAttemptContainer(logUrl: String) {}

object AppAttemptContainer {
  def apply(e: scala.xml.Elem): AppAttemptContainer = {
    AppAttemptContainer(logUrl = (e \ "logUrl").text.trim)
  }
  def listFromXml(xmlContainers: Elem): List[AppAttemptContainer] = (
    (xmlContainers \\ "container").toList.collect { case e: scala.xml.Elem =>
      apply(e)
    }
  )
}
