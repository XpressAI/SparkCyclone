package com.nec.tracing

import cats.effect.{ExitCode, IO, IOApp}
import com.comcast.ip4s.{Host, Port}
import fs2.io.net.Network
import fs2.text

import java.nio.file.{Files, Paths, StandardOpenOption}

object TracingListenerApp extends IOApp {
  val serverHost = Host.fromString("localhost")
  val serverPort = Port.fromInt(45705)

  def safeAppId(appId: String): Boolean = {
    appId.length < 100 && appId.length > 5 && appId.matches(s"^[A-Za-z0-9][A-Za-z0-9-]+")
  }

  val savingStream: Option[fs2.Pipe[IO, String, Unit]] =
    sys.env.get("LOG_DIR").map { logDir =>
      val path = Paths.get(logDir)

      stringPipe =>
        stringPipe.evalMap[IO, Unit] { str =>
          IO.delay {
            TracingRecord.parse(str) match {
              case None => System.err.println(s"Unexpected input, will ignore: ${str}")
              case Some(tracingRecord) if !safeAppId(tracingRecord.appId) =>
                System.err.println(s"Unexpected input, will ignore: ${str}")
              case Some(tracingRecord) =>
                val logFile = path.resolve(tracingRecord.appId + ".log")
                Files.write(logFile, str.getBytes(), StandardOpenOption.APPEND)
            }
          }
        }
    }

  override def run(args: List[String]): IO[ExitCode] = {
    fs2.Stream
      .resource(Network[IO].openDatagramSocket(serverHost, serverPort))
      .flatMap(_.reads)
      .evalMap(chunkBytes =>
        fs2.Stream
          .chunk[IO, Byte](chunkBytes.bytes)
          .through(text.utf8.decode)
          .compile
          .string
      )
      .through(savingStream.getOrElse(fs2.io.stdoutLines[IO, String]()))
      .compile
      .drain
      .as(ExitCode.Success)
  }
}
