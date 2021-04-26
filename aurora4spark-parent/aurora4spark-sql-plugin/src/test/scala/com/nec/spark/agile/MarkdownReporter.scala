package com.nec.spark.agile

import java.io.{BufferedReader, BufferedWriter, File, FileReader, FileWriter}
import java.nio.file.Files

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

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
      .filter(event => event.isInstanceOf[MarkupProvided])
      .map(event => {
        val markupProvided = event.asInstanceOf[MarkupProvided]
        MarkdownQueryDescription(markupProvided.text)
      })
  }

  def writeSummaryToReadme(): Unit = {
    val supportedFeaturesList = markdownDescriptions.map(description => "* " + description.value)
    val file = new File("../FEATURES.md")

    val writer = new BufferedWriter(new FileWriter(file))
    try {
      val toWriteData = Seq(
        autogenerationWarn, " ", supportedFeaturesTitle, " ") ++ supportedFeaturesList
      toWriteData.foreach(line => {
        writer.write(line)
        writer.write("\n")
      })
      writer.flush()
      writer.close()
    } finally {
      writer.close()
    }
  }
}
