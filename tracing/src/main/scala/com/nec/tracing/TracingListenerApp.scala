package com.nec.tracing

import cats.effect.{ExitCode, IO, IOApp}
import com.comcast.ip4s.{Host, Port}
import fs2.io.net.Network
import fs2.text

object TracingListenerApp extends IOApp {
  val serverHost = Host.fromString("localhost")
  val serverPort = Port.fromInt(45705)

  override def run(args: List[String]): IO[ExitCode] = {
    fs2.Stream
      .resource(Network[IO].openDatagramSocket(serverHost, serverPort))
      .flatMap(_.reads)
      .evalMap(chunkBytes =>
        fs2.Stream
          .chunk[IO, Byte](chunkBytes.bytes)
          .through(text.utf8Decode)
          .compile
          .string
      )
      .map(_.trim)
      .through(fs2.io.stdoutLines())
      .compile
      .drain
      .as(ExitCode.Success)
  }
}
