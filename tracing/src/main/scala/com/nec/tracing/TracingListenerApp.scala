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
package com.nec.tracing

import cats.effect.{ExitCode, IO, IOApp}
import com.comcast.ip4s.{Host, Port}
import cats._
import cats.implicits._
import fs2.io.net.{Network, Socket}
import fs2.text

import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import scala.collection.JavaConverters._

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

  def socketToLines(socket: Socket[IO]): IO[List[String]] =
    socket.reads
      .through(text.utf8.decode)
      .through(text.lines[IO])
      .filter(_.nonEmpty)
      .compile
      .toList

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
            socketStream.evalMap(socketToLines)
          }
          .flatMap(l => fs2.Stream.emits(l))
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
