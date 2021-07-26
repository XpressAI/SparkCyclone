package com.nec.jmh
import org.openjdk.jmh.profile.nec.StackSamplingProfiler.ThreadsSamples
import cats.effect.IO

object AnalyzeSamples {
  val IgnoreFiles = Set("Object.java", "Unsafe.java", "ThreadImpl.java")
  val TopN = 10
  def apply(threadsSamples: ThreadsSamples): IO[String] = IO{
    threadsSamples
      .flatMap(threadsSample =>
        threadsSample.threads.filter(_.stack.nonEmpty).flatMap(_.stack.headOption)
      )
      .filterNot(_.fileName.exists(f => IgnoreFiles.contains(f)))
      .groupBy(identity)
      .mapValues(_.size)
      .toList
      .sortBy(_._2)
      .reverse
      .take(TopN)
      .map { case (element, count) =>
        s"${count} ${element.fileName.map(f => f + ":" + element.lineNumber).mkString} ${element.className}#${element.methodName}"
      }
      .mkString("\n")
  }
}
