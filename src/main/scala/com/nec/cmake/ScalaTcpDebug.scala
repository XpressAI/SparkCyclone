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
  object NoOp extends ScalaTcpDebug {
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
      val suffix = s"${name}:${fullName.value}#${line.value}"
      val messageStart = s"${Instant.now()} $$ ${context} $$$$ S:${suffix}\n"
      val dsocket = new Socket(hostName, port)
      try {
        val bytesStart = messageStart.getBytes()
        dsocket.getOutputStream.write(bytesStart)
        val r = f
        val messageEnd = s"${Instant.now()} $$ ${context} $$$$ E:${suffix}\n"

        val bytesEnd = messageEnd.getBytes()
        dsocket.getOutputStream.write(bytesEnd)
        dsocket.getOutputStream.flush()
        r
      } finally dsocket.close()
    }

    override def spanIterator[T](context: String, name: String)(
      f: => Iterator[T]
    )(implicit fullName: sourcecode.FullName, line: sourcecode.Line): Iterator[T] = {
      val dsocket = new Socket(hostName, port)

      val suffix = s"${name}:${fullName.value}#${line.value}"
      List(
        Iterator
          .continually {
            val messageStart = s"${Instant.now()} $$ ${context} $$$$ S:$suffix\n"
            val bytesStart = messageStart.getBytes()

            dsocket.getOutputStream.write(bytesStart)
            None
          }
          .take(1),
        f.map(v => Some(v)),
        Iterator
          .continually {
            val messageEnd = s"${Instant.now()} $$ ${context} $$$$ E:${suffix}\n"
            val bytesEnd = messageEnd.getBytes()
            dsocket.getOutputStream.write(bytesEnd)

            dsocket.close()
            None
          }
          .take(1)
      ).toIterator.flatMap(_.flatten)
    }

  }
}
