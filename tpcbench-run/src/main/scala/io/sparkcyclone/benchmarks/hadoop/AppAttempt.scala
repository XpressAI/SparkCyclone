package io.sparkcyclone.benchmarks.hadoop

import scala.xml.Elem

final case class AppAttempt(appAttemptId: String) {}
object AppAttempt {

  def fromElem(e: Elem): AppAttempt =
    AppAttempt(appAttemptId = (e \\ "appAttemptId").text.trim)

  def listFromXml(xml: scala.xml.Elem): List[AppAttempt] =
    (xml \\ "appAttempt").collect { case e: scala.xml.Elem => fromElem(e) }.toList

}
