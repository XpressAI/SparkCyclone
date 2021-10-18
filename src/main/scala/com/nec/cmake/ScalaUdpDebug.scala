package com.nec.cmake

import com.nec.ve.VeKernelCompiler.ProfileTarget

import java.net.{DatagramPacket, InetAddress}
import java.time.Instant
import java.net.DatagramSocket

trait ScalaUdpDebug {
  def span[T](context: String, name: String)(
    f: => T
  )(implicit fullName: sourcecode.FullName, line: sourcecode.Line): T

  def spanIterator[T](context: String, name: String)(
    f: => Iterator[T]
  )(implicit fullName: sourcecode.FullName, line: sourcecode.Line): Iterator[T]
}

object ScalaUdpDebug {
  object NoOp extends ScalaUdpDebug {
    override def span[T](context: String, name: String)(
      f: => T
    )(implicit fullName: sourcecode.FullName, line: sourcecode.Line): T = f

    override def spanIterator[T](context: String, name: String)(
      f: => Iterator[T]
    )(implicit fullName: sourcecode.FullName, line: sourcecode.Line): Iterator[T] = f
  }

  object UdpTarget {
    def apply(profileTarget: ProfileTarget): UdpTarget = {
      UdpTarget(hostName = InetAddress.getByName(profileTarget.host), port = profileTarget.port)
    }
  }
  final case class UdpTarget(hostName: InetAddress, port: Int) extends ScalaUdpDebug {
    override def span[T](context: String, name: String)(
      f: => T
    )(implicit fullName: sourcecode.FullName, line: sourcecode.Line): T = {
      val suffix = s"${name}:${fullName.value}#${line.value}"
      val messageStart = s"${Instant.now()} $$ ${context} $$$$ S:${suffix}"
      val dsocket = new DatagramSocket()
      try {
        val bytesStart = messageStart.getBytes()
        val packetStart = new DatagramPacket(bytesStart, bytesStart.length, hostName, port)
        dsocket.send(packetStart)
        val r = f
        val messageEnd = s"${Instant.now()} $$ ${context} $$$$ E:${suffix}"

        val bytesEnd = messageEnd.getBytes()
        val packetEnd = new DatagramPacket(bytesEnd, bytesEnd.length, hostName, port)
        dsocket.send(packetEnd)
        r
      } finally dsocket.close()
    }

    override def spanIterator[T](context: String, name: String)(
      f: => Iterator[T]
    )(implicit fullName: sourcecode.FullName, line: sourcecode.Line): Iterator[T] = {
      val dsocket = new DatagramSocket()

      val suffix = s"${name}:${fullName.value}#${line.value}"
      List(
        Iterator
          .continually {
            val messageStart = s"${Instant.now()} $$ ${context} $$$$ S:$suffix"
            val bytesStart = messageStart.getBytes()

            val packetStart = new DatagramPacket(bytesStart, bytesStart.length, hostName, port)
            dsocket.send(packetStart)
            None
          }
          .take(1),
        f.map(v => Some(v)),
        Iterator
          .continually {
            val messageEnd = s"${Instant.now()} $$ ${context} $$$$ E:${suffix}"
            val bytesEnd = messageEnd.getBytes()
            val packetEnd = new DatagramPacket(bytesEnd, bytesEnd.length, hostName, port)
            dsocket.send(packetEnd)

            dsocket.close()
            None
          }
          .take(1)
      ).toIterator.flatMap(_.flatten)
    }

  }
}
