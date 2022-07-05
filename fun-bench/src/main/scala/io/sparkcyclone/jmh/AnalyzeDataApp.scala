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

import StackSamplingProfiler.ThreadsSamples
import scala.util.Try
import java.nio.file.Paths
import cats.effect.{ExitCode, IO, IOApp}
import fs2.io.file.Files

object AnalyzeDataApp extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    Files[IO]
      .walk(start = Paths.get("fun-bench"), maxDepth = 2)
      .filter(_.toString.endsWith("samples.json"))
      .compile
      .toList
      .flatMap { listOfSamples =>
        import cats.implicits._
        if (listOfSamples.isEmpty) IO.println("No files were found.")
        else {
          IO.println("Choose from the following files:") *> listOfSamples.zipWithIndex.traverse {
            case (path, idx) =>
              IO.println(s"[${idx + 1}] ${path}")
          } *> IO.readLine.flatMap { str =>
            Try(str.toInt).toOption.flatMap(v => listOfSamples.lift(v - 1)) match {
              case None => IO.println(s"Unknown input: '${str}', exiting.")
              case Some(sample) =>
                import io.circe.generic.auto._
                IO.println(s"Choosing '${sample}':'") *> IO
                  .blocking {
                    io.circe.parser.parse(new String(java.nio.file.Files.readAllBytes(sample)))
                  }
                  .flatMap(e => IO.fromEither(e).flatMap(e => IO.fromEither(e.as[ThreadsSamples])))
                  .flatMap { samples =>
                    AnalyzeSamples(samples).flatMap(IO.println)
                  }
            }
          }
        }
      }
      .as(ExitCode.Success)
  }
}
