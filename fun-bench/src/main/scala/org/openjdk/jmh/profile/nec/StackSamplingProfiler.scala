package org.openjdk.jmh.profile.nec

import cats.effect.{IO, Resource}
import org.openjdk.jmh.profile.nec.StackSamplingProfiler.{DefaultInterval, SamplingTask, StackTraceElementScala, ThreadSample, ThreadsSample}
import org.openjdk.jmh.infra.BenchmarkParams
import org.openjdk.jmh.infra.IterationParams
import org.openjdk.jmh.profile.InternalProfiler
import org.openjdk.jmh.results.IterationResult
import org.openjdk.jmh.results.Result
import org.openjdk.jmh.results.TextResult
import org.openjdk.jmh.runner.IterationType
import scala.concurrent.ExecutionContext.Implicits.global
import java.lang.management.ManagementFactory
import java.nio.file.Paths
import java.time.Instant
import java.util
import java.util.Collections

import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.language.existentials
import scala.util.Try

import io.circe.{Decoder, Encoder}
import io.circe.generic.{JsonCodec, semiauto}
import io.circe.syntax._

object StackSamplingProfiler {

  type ThreadsSamples = List[ThreadsSample]
  implicit val globalTimer = IO.timer(global)
  case class ThreadsSample(instant: Instant, threads: List[ThreadSample])
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
    implicit val globalShift = IO.contextShift(global)
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

        val stream = fs2.Stream
          .awakeEvery[IO](interval)
          .evalMap { _ =>
            getSample
          }
          .evalMap(sample => IO.delay(collectedItems.append(sample)))
          .compile
          .drain

      val (allocated, end) = Resource.make(stream.start)(_.cancel).map(_.join)
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
      import JsonCodecs._
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
object JsonCodecs {
  implicit val listStackTracesEncoder: Encoder[List[StackTraceElementScala]] = Encoder
    .encodeList(
      Encoder
        .forProduct4("className", "methodName", "fileName", "lineNumber")(
          prod =>(prod.className, prod.methodName, prod.fileName, prod.lineNumber)
        )
    )
  implicit val sampleEncoder: Encoder[ThreadSample] = Encoder.forProduct2("threadName", "stack")(prod =>
    (prod.threadName, prod.stack))
  implicit val instantEncoder: Encoder[Instant] = Encoder.encodeString.contramap[Instant](_.toString)
  implicit val samplesEncoder: Encoder[ThreadsSample] = Encoder.forProduct2("instant", "threads")(prod =>
    (prod.instant, prod.threads))
  implicit val listEncoder: Encoder[List[ThreadsSample]] = Encoder.encodeList[ThreadsSample]

  implicit val listStackTracesDecoder: Decoder[List[StackTraceElementScala]] = Decoder
    .decodeList(
      Decoder
        .forProduct4("className", "methodName", "fileName", "lineNumber")(StackTraceElementScala.apply))


  implicit val sampleDecoder: Decoder[ThreadSample] = Decoder.forProduct2("threadName", "stack")(ThreadSample.apply)
  implicit val instantDecoder: Decoder[Instant] = Decoder.decodeString.emapTry { str =>
    Try(Instant.parse(str))
  }
  implicit val samplesDecoder: Decoder[ThreadsSample] = Decoder.forProduct2("instant", "threads")(ThreadsSample.apply)
  implicit val listDecoder: Decoder[List[ThreadsSample]] = Decoder.decodeList[ThreadsSample]

}