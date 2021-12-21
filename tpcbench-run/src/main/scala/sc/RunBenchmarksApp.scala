package sc

import cats.effect.{Deferred, ExitCode, IO, IOApp}
import com.comcast.ip4s.Host
import com.nec.tracing.SpanProcessor
import com.nec.tracing.TracingListenerApp.socketToLines
import fs2.concurrent.{Signal, SignallingRef}
import fs2.io.net.Network
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.scalaxml.xml
import sc.RunOptions.RunResult
import sc.hadoop.AppsContainer

import java.nio.file.Paths
import java.time.{Duration, Instant}
import scala.concurrent.duration.DurationInt

object RunBenchmarksApp extends IOApp {

  final case class RunResults(appUrl: String, traceResults: String)

  private def getApps: IO[AppsContainer] = {
    BlazeClientBuilder[IO].resource
      .use(client => client.expect[scala.xml.Elem](uri"http://localhost:8088/ws/v1/cluster/apps"))
      .map(elem => AppsContainer.parse(elem))
  }

  private def runCommand(runOptions: RunOptions, doTrace: Boolean): IO[(Int, RunResults)] =
    Network[IO].serverResource(address = Host.fromString("0.0.0.0")).use {
      case (ipAddr, streamOfSockets) =>
        def runProc = IO.blocking {
          val sparkHome = "/opt/spark"
          import scala.sys.process._
          val command = Seq(s"$sparkHome/bin/spark-submit") ++ {
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
                .apply(fout = s => System.out.println(s), ferr = s => System.err.println(s))
            )
          proc -> IO
            .pure(proc)
            .onCancel(IO.blocking(proc.destroy()))
            .flatMap(proc => IO.blocking(proc.exitValue()))
        }

        /** Start the tracer, and then the process, then wait for the process to complete and then end the stream of the tracer, */
        for {
          s <- SignallingRef[IO].of(false)
          streamFiber <- streamOfSockets
            .evalMap(socketToLines)
            .interruptWhen(haltWhenTrue = s)
            .compile
            .toList
            .map(_.flatten)
            .start
          initialApps <- getApps
          prio <- runProc
          proc = prio._1
          procCloseIO = prio._2
          procFiber <- procCloseIO.start
          afterStartApps <- getApps.delayBy(15.seconds)
          /** to allow spark to create a tracking url */
          newFoundApps = afterStartApps.apps.filter(app => !initialApps.apps.exists(_.id == app.id))
          newFoundApp = newFoundApps.headOption
            .getOrElse(sys.error(s"Could not find Hadoop app for this"))
          _ <- {
            import cats.implicits._
            newFoundApps.traverse { newFoundApp =>
              IO.println(show"Hadoop app found:") *> IO.println(newFoundApp)
            }
          }

          exitValue <- procFiber.joinWithNever
          _ <- s.set(true).delayBy(2.seconds)
          traceLines <- streamFiber.joinWithNever
          _ = println(s"Trace lines => ${traceLines.toString().take(50)}")
          analyzeResult = SpanProcessor.analyzeLines(traceLines)
        } yield (exitValue, RunResults(newFoundApp.appUrl, analyzeResult.mkString("", "\n", "")))
    }

  override def run(args: List[String]): IO[ExitCode] = {
    import doobie._
    import doobie.implicits._
    import cats._
    import cats.data._
    import cats.effect.IO
    import cats.implicits._

    val uri = "jdbc:sqlite:/tmp/benchmark-results.db"
    val xa = Transactor
      .fromDriverManager[IO]("org.sqlite.JDBC", uri, "", "")

    val allOptions: List[RunOptions] = {
      if (args.contains("--query=all")) {
        val runId: String = java.time.Instant.now().toString
        (1 to 22).map { qId =>
          val cleanRunId: String = runId.filter(char => Character.isLetterOrDigit(char))
          args
            .foldLeft(
              RunOptions.default
                .copy(
                  runId = s"${runId}_${qId}",
                  name = Some(s"Benchmark_${cleanRunId}_${qId}"),
                  queryNo = qId
                )
            ) { case (ro, arg) =>
              ro.rewriteArgs(arg).getOrElse(ro)
            }
        }.toList
      } else {
        val runId: String = java.time.Instant.now().toString
        val cleanRunId: String = runId.filter(char => Character.isLetterOrDigit(char))
        List(
          args
            .foldLeft(
              RunOptions.default.copy(runId = runId, name = Some(s"Benchmark_${cleanRunId}"))
            ) { case (ro, arg) =>
              ro.rewriteArgs(arg).getOrElse(ro)
            }
        )
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
                queryTime = wallTime,
                appUrl = traceResults.appUrl,
                traceResults = traceResults.traceResults
              )
            ) *> rd.fetchResults.flatMap(_.save)
        }
      )
      .void
      .as(ExitCode.Success)
  }
}
