/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.sparkcyclone.acceptance

import io.sparkcyclone.acceptance.MarkdownReporter.MarkdownQueryDescription

import java.nio.file.Files
import java.nio.file.Paths
import org.scalatest.Reporter
import org.scalatest.events.Event
import org.scalatest.events.MarkupProvided
import org.scalatest.events.RecordableEvent
import org.scalatest.events.RunCompleted
import org.scalatest.events.TestSucceeded

import java.nio.charset.StandardCharsets

object MarkdownReporter {
  final case class MarkdownQueryDescription(value: String) extends AnyVal
}

final class MarkdownReporter extends Reporter {
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
      .collect { case MarkupProvided(_, text, _, _, _, _, _, _) =>
        MarkdownQueryDescription(text)
      }
  }

  def writeSummaryToReadme(): Unit = {
    val supportedFeaturesList = markdownDescriptions.map(description => "* " + description.value)
    val toWriteData =
      Seq(autogenerationWarn, " ", supportedFeaturesTitle, " ") ++ supportedFeaturesList
    Files.write(
      Paths.get("..").resolve("FEATURES.md"),
      toWriteData.mkString("\n").getBytes(StandardCharsets.UTF_8)
    )
  }
}
