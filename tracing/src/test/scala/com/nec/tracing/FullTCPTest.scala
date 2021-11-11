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

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.comcast.ip4s.{Host, Port}
import com.eed3si9n.expecty.Expecty.expect
import com.nec.cmake.ScalaTcpDebug
import com.nec.tracing.TracingListenerApp.socketToLines
import com.nec.ve.VeKernelCompiler.ProfileTarget
import fs2.io.net.Network
import org.scalatest.freespec.AnyFreeSpec

import scala.concurrent.duration.DurationInt

final class FullTCPTest extends AnyFreeSpec {
  val tcpTarget: ScalaTcpDebug.TcpTarget =
    ScalaTcpDebug.TcpTarget(ProfileTarget("127.0.0.1", 12345))
  "It works" in {
    val strings = {
      fs2.Stream
        .resource(
          Network[IO]
            .serverResource(address = Host.fromString("127.0.0.1"), port = Port.fromString("12345"))
        )
        .flatMap { case (socketAddress, socketStream) =>
          fs2.Stream
            .eval(IO.delay(tcpTarget.span("test", "test2")(123)))
            .drain
            .covaryOutput[String]
            .merge(
              socketStream
                .take(1)
                .evalMap(socketToLines)
                .flatMap(l => fs2.Stream.emits(l))
            )
        }
        .compile
        .toList
    }.timeout(5.seconds).unsafeRunSync()

    expect(
      strings.size == 2,
      strings.exists(_.contains("S:test2")),
      strings.exists(_.contains("E:test2"))
    )
  }
}
