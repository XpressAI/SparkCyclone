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
package com.nec.cmake

import com.nec.ve.VeKernelCompiler.ProfileTarget

import java.net.{DatagramPacket, DatagramSocket, InetAddress, Socket}
import java.time.Instant

trait ScalaTcpDebug {
  def span[T](context: String, name: String)(
    f: => T
  )(implicit fullName: sourcecode.FullName, line: sourcecode.Line): T

  def spanIterator[T](context: String, name: String)(
    f: => Iterator[T]
  )(implicit fullName: sourcecode.FullName, line: sourcecode.Line): Iterator[T]
}

object ScalaTcpDebug {
  object NoOp extends ScalaTcpDebug with Serializable {
    override def span[T](context: String, name: String)(
      f: => T
    )(implicit fullName: sourcecode.FullName, line: sourcecode.Line): T = f

    override def spanIterator[T](context: String, name: String)(
      f: => Iterator[T]
    )(implicit fullName: sourcecode.FullName, line: sourcecode.Line): Iterator[T] = f
  }

  object TcpTarget {
    def apply(profileTarget: ProfileTarget): TcpTarget = {
      TcpTarget(hostName = InetAddress.getByName(profileTarget.host), port = profileTarget.port)
    }
  }
  final case class TcpTarget(hostName: InetAddress, port: Int) extends ScalaTcpDebug {
    override def span[T](context: String, name: String)(
      f: => T
    )(implicit fullName: sourcecode.FullName, line: sourcecode.Line): T = {
      /*val suffix = s"${name}:${fullName.value}#${line.value}"
      val messageStart = s"${Instant.now()} $$ ${context} $$$$ S:${suffix}\n"
      val dsocket = new Socket(hostName, port)
      try {
        val bytesStart = messageStart.getBytes()
        dsocket.getOutputStream.write(bytesStart)*/
        val r = f
        /*val messageEnd = s"${Instant.now()} $$ ${context} $$$$ E:${suffix}\n"

        val bytesEnd = messageEnd.getBytes()
        dsocket.getOutputStream.write(bytesEnd)
        dsocket.getOutputStream.flush()*/
        r
      /*} finally dsocket.close()*/
    }

    override def spanIterator[T](context: String, name: String)(
      f: => Iterator[T]
    )(implicit fullName: sourcecode.FullName, line: sourcecode.Line): Iterator[T] = {
      /*val dsocket = new Socket(hostName, port)

      val suffix = s"${name}:${fullName.value}#${line.value}"*/
      List(
        /*Iterator
          .continually {
            val messageStart = s"${Instant.now()} $$ ${context} $$$$ S:$suffix\n"
            val bytesStart = messageStart.getBytes()

            dsocket.getOutputStream.write(bytesStart)
            None
          }
          .take(1),*/
        f.map(v => Some(v)),
        /*Iterator
          .continually {
            val messageEnd = s"${Instant.now()} $$ ${context} $$$$ E:${suffix}\n"
            val bytesEnd = messageEnd.getBytes()
            dsocket.getOutputStream.write(bytesEnd)

            dsocket.close()
            None
          }
          .take(1)
      */).toIterator.flatMap(_.flatten)
    }

  }
}
