package com.nec.spark.agile

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

import org.apache.commons.io.Charsets
import org.scalatest.Reporter
import org.scalatest.events.{Event, MarkupProvided, RecordableEvent, RunCompleted, TestSucceeded}

case class MarkdownQueryDescription(value: String) extends AnyVal
class MarkdownReporter extends Reporter {
  var markdownDescriptions = Set.empty[MarkdownQueryDescription]
  private val autogenerationWarn =
    """## This file has been generated automatically.
      |Do not modify it manually.""".stripMargin
  private val supportedFeaturesTitle = "## Currently supported queries"

  override def apply(event: Event): Unit = event match {
    case TestSucceeded(_, _, _, _, _, _, recordedEvents, _, _, _, _, _, _, _) =>
      markdownDescriptions = markdownDescriptions ++ extractMarkdown(recordedEvents)
    case RunCompleted(_, _, _, _, _, _, _, _) => writeSummaryToReadme()
    // format:off
    case _ => ()
  }

  def extractMarkdown(recordedEvents: Seq[RecordableEvent]): Seq[MarkdownQueryDescription] = {
    recordedEvents
      .collect {
        case MarkupProvided(_, text, _, _, _, _, _, _) => MarkdownQueryDescription(text)}
  }

  def writeSummaryToReadme(): Unit = {
    val supportedFeaturesList = markdownDescriptions.map(description => "* " + description.value)
    val toWriteData = Seq(
      autogenerationWarn, " ", supportedFeaturesTitle, " ") ++ supportedFeaturesList
    Files.write(Paths.get("..").resolve("FEATURES.md"),
      toWriteData.mkString("\n").getBytes(Charsets.UTF_8))
  }
}
