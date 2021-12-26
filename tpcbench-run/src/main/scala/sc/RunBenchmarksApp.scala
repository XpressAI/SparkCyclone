package sc

import cats.effect.unsafe.IORuntime
import cats.effect.unsafe.implicits.global
import cats.effect.{ExitCode, IO, IOApp}
import ch.qos.logback.classic.spi.{ILoggingEvent, LoggingEventVO}
import com.comcast.ip4s.Host
import com.nec.tracing.SpanProcessor
import com.nec.tracing.TracingListenerApp.socketToLines
import fs2.concurrent.{SignallingRef, Topic}
import fs2.io.net.Network
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.scalaxml.xml
import sc.DetectLogback.LogbackItemsClasspath
import sc.RunResults.DefaultOrdering
import sc.RunOptions.PosixPermissions
import sc.hadoop.{AppAttempt, AppAttemptContainer, AppsContainer}

import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermissions
import java.time.{Duration, Instant}
import scala.concurrent.duration.DurationInt
import scala.language.higherKinds

object RunBenchmarksApp extends IOApp {

  final case class RunResults(
    appUrl: String,
    compileTime: Option[String],
    queryTime: Option[String],
    traceResults: String,
    logOutput: String,
    containerList: List[String],
    metrics: List[String],
    maybeFoundPlan: Option[String]
  )

  private def getApps: IO[AppsContainer] = {
    BlazeClientBuilder[IO].resource
      .use(client => client.expect[scala.xml.Elem](uri"http://localhost:8088/ws/v1/cluster/apps"))
      .map(elem => AppsContainer.parse(elem))
  }

  private def fetchContainers(appId: String): IO[List[AppAttemptContainer]] = {
    import cats._
    import cats.implicits._
    import cats.syntax._
    val startUri =
      uri"http://localhost:8088/ws/v1/cluster/apps/".addSegment(appId).addSegment("appattempts")
    BlazeClientBuilder[IO].resource
      .use(client =>
        client
          .expect[scala.xml.Elem](startUri)
          .map(AppAttempt.listFromXml)
          .flatMap(listOfAttempts =>
            listOfAttempts.traverse(attempt => {
              val attUri = uri"http://localhost:8088/ws/v1/cluster/apps/"
                .addSegment(appId)
                .addSegment("appattempts")
                .addSegment(attempt.appAttemptId)
                .addSegment("containers")
              client
                .expect[scala.xml.Elem](attUri)
                .map(AppAttemptContainer.listFromXml)
            })
          )
      )
      .map(_.flatten)
  }

  def getDistinct[F[_], I]: fs2.Pipe[F, I, I] =
    _.scan(Set.empty[I] -> Option.empty[I]) {
      case ((s, _), i) if s.contains(i) => (s, None)
      case ((s, _), i)                  => (s + i, Some(i))
    }.map(_._2).unNone

  private def monitorAllContainers(appId: String): fs2.Stream[IO, AppAttemptContainer] =
    fs2.Stream
      .awakeEvery[IO](5.seconds)
      .flatMap(_ => fs2.Stream.evalSeq(fetchContainers(appId)))
      .through(getDistinct[IO, AppAttemptContainer])

  require(LogbackItemsClasspath.nonEmpty, "Expecting to have logback in classpath.")

  private def runCommand(runOptions: RunOptions, doTrace: Boolean)(implicit
    runtime: IORuntime
  ): IO[(Int, RunResults)] =
    Network[IO].serverResource(address = Host.fromString("0.0.0.0")).use {
      case (ipAddr, streamOfSockets) =>
        Network[IO].serverResource(address = Host.fromString("0.0.0.0")).use {
          case (logbackIpAddr, logbackStreamOfSockets) =>
            def runProc(_stdout: String => IO[Unit], _stderr: String => IO[Unit]) = IO.blocking {
              val sparkHome = "/opt/spark"
              import scala.sys.process._

              val tempFileLocation = Files.createTempFile(
                "logback",
                ".xml",
                PosixFilePermissions.asFileAttribute(PosixPermissions)
              )

              Files.write(
                tempFileLocation,
                BenchmarkLogbackConfigurationFile
                  .forPort(logbackIpAddr.port.value)
                  .toString
                  .getBytes()
              )

              val logbackConf =
                List(
                  "--conf",
                  s"spark.executor.extraJavaOptions=-Dlogback.configurationFile=${tempFileLocation}",
                  "--driver-java-options",
                  s"-Dlogback.configurationFile=${tempFileLocation}"
                )

              val metricsConf =
                List(
                  "*.sink.slf4j.class=org.apache.spark.metrics.sink.Slf4jSink",
                  "*.sink.slf4j.period=5"
                )

              val metricsOptions: List[String] = {
                metricsConf.flatMap(item => List("--conf", s"spark.metrics.conf.${item}"))
              }

              val command =
                Seq(s"$sparkHome/bin/spark-submit") ++ metricsOptions ++ logbackConf ++ {
                  if (doTrace)
                    List(
                      "--conf",
                      s"spark.com.nec.spark.ncc.profile-target=127.0.0.1:${ipAddr.port.value}"
                    )
                  else Nil
                } ++
                  runOptions.toArguments

              println(s"Running command: ${command.mkString(" ")}")
              val proc = Process(command = command, None, "SPARK_HOME" -> sparkHome)
                .run(
                  ProcessLogger
                    .apply(
                      fout = s => _stdout(s).unsafeRunSync()(runtime),
                      ferr = s => _stderr(s).unsafeRunSync()(runtime)
                    )
                )
              proc -> IO
                .pure(proc)
                .onCancel(IO.blocking(proc.destroy()))
                .flatMap(proc => IO.blocking(proc.exitValue()))
            }

            /** Start the tracer, and then the process, then wait for the process to complete and then end the stream of the tracer, */
            for {
              s <- SignallingRef[IO].of(false)
              traceStreamFiber <- streamOfSockets
                .evalMap(socketToLines)
                .interruptWhen(haltWhenTrue = s)
                .compile
                .toList
                .map(_.flatten)
                .start
              initialApps <- getApps
              outLinesTopic <- Topic[IO, String]
              outLinesF <- outLinesTopic
                .subscribe(Int.MaxValue)
                .interruptWhen(haltWhenTrue = s)
                .compile
                .toList
                .start
              logLinesF <- logbackStreamOfSockets
                .evalTap(socket => IO.println(s"$socket connected!"))
                .map(socket => LogbackListener.getLoggingEvents(socket))
                .parJoinUnbounded
                .map((i: ILoggingEvent) => i.asInstanceOf[LoggingEventVO])
                .filter(i =>
                  i.getLoggerName != null && (i.getLoggerName.contains(".nec.") || i.getLoggerName
                    .contains("sparkcyclone"))
                )
                .interruptWhen(haltWhenTrue = s)
                .evalTap(event => IO.println(s"${event}: ${event.getMessage}"))
                .compile
                .toList
                .start
              prio <- runProc(
                _stdout = line => IO.println(line) *> outLinesTopic.publish1(line).void,
                _stderr = line =>
                  IO.println(line)
                    *> outLinesTopic.publish1(line).void
              )
              proc = prio._1
              procCloseIO = prio._2
              procFiber <- procCloseIO.start
              afterStartApps <- getApps.delayBy(15.seconds)

              /** to allow spark to create a tracking url */
              newFoundApps = afterStartApps.apps
                .filter(app => !initialApps.apps.exists(_.id == app.id))
              newFoundApp = newFoundApps.headOption
                .getOrElse(sys.error(s"Could not find Hadoop app for this"))
              _ <- {
                import cats.implicits._
                newFoundApps.traverse { newFoundApp =>
                  IO.println(show"Hadoop app found:") *> IO.println(newFoundApp)
                }
              }

              containerLogsListF <- monitorAllContainers(newFoundApp.id)
                .interruptWhen(haltWhenTrue = s)
                .evalTap(cnt => IO.println(s"Detected a new container: ${cnt.logUrl}"))
                .compile
                .toList
                .start
              exitValue <- procFiber.joinWithNever
              _ <- s.set(true).delayBy(2.seconds)
              traceLines <- traceStreamFiber.joinWithNever
              outLines <- outLinesF.joinWithNever
              loggingEventVoes <- logLinesF.joinWithNever
              maybeFoundPlan = loggingEventVoes.collectFirst {
                case logEvent if logEvent.getMessage.startsWith("Final plan:") =>
                  logEvent.getMessage
              }
              containerLogsList <- containerLogsListF.joinWithNever
              _ = println(s"Trace lines => ${traceLines.toString().take(50)}")
              analyzeResult = SpanProcessor.analyzeLines(traceLines)
            } yield (
              exitValue,
              RunResults(
                appUrl = newFoundApp.appUrl,
                traceResults = analyzeResult.mkString("", "\n", ""),
                logOutput = outLines.mkString("", "\n", ""),
                containerList = containerLogsList.map(_.logUrl).sorted,
                metrics = loggingEventVoes.collect {
                  case m if m.getMessage != null && MetricCapture.matches(m.getMessage) =>
                    m.getMessage
                },
                maybeFoundPlan = maybeFoundPlan,
                compileTime = loggingEventVoes
                  .flatMap(m => StringUtils.afterStart(m.getMessage, "Compilation time: "))
                  .headOption,
                queryTime = loggingEventVoes
                  .flatMap(m => StringUtils.afterStart(m.getMessage, "Query time: "))
                  .headOption
              )
            )
        }
    }

  override def run(args: List[String]): IO[ExitCode] = {
    import cats.effect.IO
    import cats.implicits._
    import doobie._

    val uri = "jdbc:sqlite:/tmp/benchmark-results.db"
    val xa = Transactor
      .fromDriverManager[IO]("org.sqlite.JDBC", uri, "", "")

    val allOptions: List[RunOptions] = {
      if (args.contains("--query=all")) {
        val runId: String = java.time.Instant.now().toString
        (1 to 22).map { qId =>
          val cleanRunId: String = runId.filter(char => Character.isLetterOrDigit(char))
          val initial = RunOptions.default
            .copy(
              runId = s"${runId}_${qId}",
              name = Some(s"Benchmark_${cleanRunId}_${qId}"),
              queryNo = qId
            )

          initial.enhanceWith(args).enhanceWithEnv(sys.env)
        }.toList
      } else {
        val runId: String = java.time.Instant.now().toString
        val cleanRunId: String = runId.filter(char => Character.isLetterOrDigit(char))
        val initial =
          RunOptions.default.copy(runId = runId, name = Some(s"Benchmark_${cleanRunId}"))
        List(initial.enhanceWith(args).enhanceWithEnv(sys.env))
      }
    }

    def time[T](what: IO[T]): IO[(T, Int)] = {
      for {
        start <- IO.delay(Instant.now())
        r <- what
        end <- IO.delay(Instant.now())
      } yield (r, Duration.between(start, end).getSeconds.toInt)
    }

    val rd = RunDatabase(xa, uri)
    allOptions
      .traverse(options =>
        time(runCommand(options, doTrace = false)).flatMap {
          case ((result, traceResults), wallTime) =>
            rd.initialize *> rd.insert(
              options,
              RunResult(
                succeeded = result == 0,
                wallTime = wallTime,
                queryTime = traceResults.queryTime.getOrElse(""),
                compileTime = traceResults.compileTime.getOrElse(""),
                appUrl = traceResults.appUrl,
                traceResults = traceResults.traceResults,
                logOutput = traceResults.logOutput,
                containerList = traceResults.containerList.mkString("\n"),
                metrics = traceResults.metrics.mkString("\n"),
                finalPlan = traceResults.maybeFoundPlan
              )
            ) *> rd.fetchResults.map(_.reorder(DefaultOrdering)).flatMap(_.save)
        }
      )
      .void
      .as(ExitCode.Success)
  }
}
