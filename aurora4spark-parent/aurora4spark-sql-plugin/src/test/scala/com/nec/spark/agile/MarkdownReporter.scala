package com.nec.spark.agile

import java.io.{BufferedReader, BufferedWriter, File, FileReader, FileWriter}
import java.nio.file.Files

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import org.scalatest.Reporter
import org.scalatest.events.{Event, MarkupProvided, RecordableEvent, RunCompleted, TestSucceeded}

case class MarkdownQueryDescription(value: String) extends AnyVal
class MarkdownReporter extends Reporter {
  var markdownDescriptions = Set.empty[MarkdownQueryDescription]
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
    try {
      val file = new File("README.md")
      val lines = Files.readAllLines(file.toPath)
      val splitAtIndex = lines.indexOf(supportedFeaturesTitle)

      val (rewrittenPart, toUpdatePart) = lines.asScala.splitAt(splitAtIndex)

      val rewrittenPart2 = toUpdatePart.drop(2).dropWhile(line => line.startsWith("*"))

      val writer = new BufferedWriter(new FileWriter(file))

      val readMeLines =
        (rewrittenPart.toSeq :+ supportedFeaturesTitle) ++ supportedFeaturesList ++ rewrittenPart2

      readMeLines.foreach(line => {
        writer.write(line)
        writer.write("\n")
      })
      writer.flush()
      writer.close()
    }
  }
}
