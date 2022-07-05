package io.sparkcyclone.benchmarks

import cats.effect.IO
import ch.qos.logback.classic.spi.ILoggingEvent
import fs2.io.net.Socket

import java.io.ObjectInputStream

object LogbackListener {
  def getLoggingEvents(socket: Socket[IO]): fs2.Stream[IO, ILoggingEvent] =
    fs2.Stream
      .resource(fs2.io.toInputStreamResource(socket.reads))
      .map(inputStream => new ObjectInputStream(inputStream))
      .flatMap(ois =>
        fs2.Stream
          .repeatEval(IO.blocking(ois.readObject().asInstanceOf[ILoggingEvent]))
          .handleErrorWith {
            case _: java.io.EOFException => fs2.Stream.empty
            case other                   => throw other
          }
      )
}
