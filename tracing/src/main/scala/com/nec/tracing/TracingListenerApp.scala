package com.nec.tracing

import cats.effect.{ExitCode, IO, IOApp}
import com.comcast.ip4s.{Host, Port}
import cats._
import cats.implicits._
import fs2.io.net.Network
import fs2.text

import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import scala.jdk.CollectionConverters.ListHasAsScala

object TracingListenerApp extends IOApp {
  val serverHost = Host.fromString(sys.env.getOrElse("TRACING_HOST", "0.0.0.0"))
  val serverPort = Port.fromInt(sys.env.get("TRACING_PORT").map(_.toInt).getOrElse(45705))

  def safeAppId(appId: String): Boolean = {
    appId.length < 100 && appId.length > 5 && appId.matches(s"^[A-Za-z0-9][A-Za-z0-9-_]+")
  }

  val savingStream: Option[fs2.Pipe[IO, String, Unit]] =
    sys.env.get("LOG_DIR").map(ld => Paths.get(ld)).map { logDir => stringPipe =>
      stringPipe.evalMap[IO, Unit] { str =>
        IO.delay {
          TracingRecord.parse(str) match {
            case None => System.err.println(s"Unexpected input, will ignore: ${str}")
            case Some(tracingRecord) if !safeAppId(tracingRecord.appId) =>
              System.err.println(s"Unexpected input, will ignore: ${str}")
            case Some(tracingRecord) =>
              val logFile = logDir.resolve(tracingRecord.appId + ".log")
              if (!Files.exists(logDir)) Files.createDirectory(logDir)
              if (!Files.exists(logFile)) Files.createFile(logFile)
              Files.write(logFile, s"${str.trim}\n".getBytes(), StandardOpenOption.APPEND)
          }
        }
      }
    }

  override def run(args: List[String]): IO[ExitCode] = {
    val fileToAnalyze =
      if (args.contains("analyze"))
        args.dropWhile(_ != "analyze").drop(1).headOption.map(f => Paths.get(f))
      else None

    fileToAnalyze match {
      case None =>
        fs2.Stream
          .resource(Network[IO].serverResource(address = serverHost, port = serverPort))
          .flatMap { case (socketAddress, socketStream) =>
            socketStream.evalMap(_.reads.through(text.utf8.decode).compile.string)
          }
          .through(savingStream.getOrElse(fs2.io.stdoutLines[IO, String]()))
          .compile
          .drain
          .as(ExitCode.Success)
      case Some(file) =>
        analyzeFile(file)
    }
  }

  def analyzeFile(file: Path): IO[ExitCode] = {
    IO.blocking(Files.readAllLines(file).asScala)
      .flatMap(lines => IO.blocking(SpanProcessor.analyzeLines(lines.toList)))
      .flatMap(lines => lines.traverse(l => IO.println(l)))
      .as(ExitCode.Success)
  }
}
