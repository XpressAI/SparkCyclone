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
package io.sparkcyclone.jmh

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import StackSamplingProfiler.DefaultInterval
import StackSamplingProfiler.SamplingTask
import org.openjdk.jmh.infra.BenchmarkParams
import org.openjdk.jmh.infra.IterationParams
import org.openjdk.jmh.profile.InternalProfiler
import org.openjdk.jmh.results.IterationResult
import org.openjdk.jmh.results.Result
import org.openjdk.jmh.results.TextResult
import org.openjdk.jmh.runner.IterationType

import java.lang.management.ManagementFactory
import java.nio.file.Paths
import java.time.Instant
import java.util
import java.util.Collections
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.language.existentials

object StackSamplingProfiler {

  type ThreadsSamples = List[ThreadsSample]
  final case class ThreadsSample(instant: Instant, threads: List[ThreadSample])
  final case class ThreadSample(threadName: String, stack: List[StackTraceElementScala])
  final case class StackTraceElementScala(
    className: String,
    methodName: String,
    fileName: Option[String],
    lineNumber: Int
  ) {
    require(className != null)
    require(methodName != null)
    require(fileName != null)
  }
  object StackTraceElementScala {
    def apply(stackTraceElement: StackTraceElement): StackTraceElementScala =
      StackTraceElementScala(
        className = stackTraceElement.getClassName,
        methodName = stackTraceElement.getMethodName,
        fileName = Option(stackTraceElement.getFileName),
        lineNumber = stackTraceElement.getLineNumber
      )
  }

  final class SamplingTask(interval: FiniteDuration) {
    var end: IO[Unit] = _

    def stop(): Unit = end.unsafeRunSync()
    def getSample: IO[ThreadsSample] = IO.delay {
      val threadInfos = ManagementFactory.getThreadMXBean.dumpAllThreads(false, false)
      ThreadsSample(
        instant = Instant.now(),
        threads = threadInfos.toList.map(threadInfo =>
          ThreadSample(
            threadName = threadInfo.getThreadName,
            stack = threadInfo.getStackTrace.toList.map(stackTraceElement =>
              StackTraceElementScala(stackTraceElement)
            )
          )
        )
      )
    }
    var collectedItems = scala.collection.mutable.Buffer.empty[ThreadsSample]
    def start(): Unit = {
      val (allocated, end) =
        fs2.Stream
          .awakeEvery[IO](interval)
          .evalMap { _ =>
            getSample
          }
          .evalMap(sample => IO.delay(collectedItems.append(sample)))
          .compile
          .drain
          .background
          .allocated
          .unsafeRunSync()

      this.end = end
    }
    def result(benchmarkParams: BenchmarkParams): TextResult = {
      val theFile = Paths
        .get(System.getProperty("user.dir"))
        .resolve(benchmarkParams.id())
        .resolve("thread-samples.json")
        .toAbsolutePath
      import io.circe.syntax._
      import io.circe.generic.auto._
      java.nio.file.Files.write(theFile, collectedItems.toList.asJson.spaces2.getBytes())
      new TextResult(s"Saved a log of thread samples to file: ${theFile}", "ssp")
    }
  }

  val DefaultInterval: FiniteDuration = 1.second

}

final class StackSamplingProfiler(params: Option[String]) extends InternalProfiler {

  def this(str: String) = this(Some(str))

  def this() = this(None)

  @volatile
  private var samplingTask: SamplingTask = _

  override def beforeIteration(
    benchmarkParams: BenchmarkParams,
    iterationParams: IterationParams
  ): Unit = {
    import scala.concurrent.duration._

    this.samplingTask = new SamplingTask(
      params
        .filter(_.nonEmpty)
        .map(strInterval => Duration(strInterval).toSeconds)
        .filter(_ > 0)
        .map(intervalSecs => FiniteDuration(intervalSecs, SECONDS))
        .getOrElse(DefaultInterval)
    )
    this.samplingTask.start()
  }

  override def afterIteration(
    benchmarkParams: BenchmarkParams,
    iterationParams: IterationParams,
    result: IterationResult
  ): util.Collection[
    _ <: Result[T] forSome {
      type T <: Result[T]
    }
  ] = {
    this.samplingTask.stop()
    if (iterationParams.getType == IterationType.MEASUREMENT) {
      val result = Collections.singleton(this.samplingTask.result(benchmarkParams))
      this.samplingTask.collectedItems.clear()
      result
    } else {
      Collections.emptyList()
    }
  }

  override def getDescription: String =
    "Java stack profiler to collect stack traces periodically for a picture of activity"
}
