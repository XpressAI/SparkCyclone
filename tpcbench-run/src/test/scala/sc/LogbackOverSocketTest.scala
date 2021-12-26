package sc

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.net.SocketAppender
import ch.qos.logback.core.ConsoleAppender
import com.nec.tracing.TracingListenerApp.{serverHost, serverPort, socketToLines}
import fs2.io.net.Network
import com.comcast.ip4s.{Host, Port}
import fs2.concurrent.SignallingRef
import org.scalatest.freespec.AnyFreeSpec
import org.slf4j.LoggerFactory

import java.io.ObjectInputStream
import scala.concurrent.duration.DurationInt

final class LogbackOverSocketTest extends AnyFreeSpec {
  "It works" in {
    import ch.qos.logback.classic.LoggerContext
    import ch.qos.logback.classic.encoder.PatternLayoutEncoder
    import ch.qos.logback.classic.spi.ILoggingEvent
    import ch.qos.logback.classic.Logger

    val lc = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    val ple = new PatternLayoutEncoder

    ple.setPattern("%date %level [%thread] %logger{10} [%file:%line] %msg%n")
    ple.setContext(lc)
    ple.start()
    val consoleAppender: ConsoleAppender[ILoggingEvent] = new ConsoleAppender[ILoggingEvent]
    consoleAppender.setEncoder(ple)
    consoleAppender.setContext(lc)
    consoleAppender.start()

    val completion = SignallingRef.of[IO, Boolean](false).unsafeRunSync()

    val qq = Network[IO]
      .serverResource(address = Host.fromString("127.0.0.1"), port = Port.fromInt(9991))
      .use { case (socketAddress, socketStream) =>
        socketStream
          .flatMap(socket =>
            fs2.Stream
              .resource(fs2.io.toInputStreamResource(socket.reads))
              .map(inputStream => new ObjectInputStream(inputStream))
              .interruptWhen(haltWhenTrue = completion)
              .flatMap(ois =>
                fs2.Stream
                  .repeatEval(IO.blocking(ois.readObject().asInstanceOf[ILoggingEvent]))
                  .handleErrorWith {
                    case _: java.io.EOFException => fs2.Stream.empty
                    case other                   => throw other
                  }
              )
              .evalMap(e => IO.println(e.toString))
          )
          .interruptWhen(haltWhenTrue = completion)
          .compile
          .drain
      }

    val stt = qq.start.unsafeRunSync()

    val socketAppender = new SocketAppender()
    socketAppender.setContext(lc)
    socketAppender.setRemoteHost("127.0.0.1")
    socketAppender.setPort(9991)
    socketAppender.start()

    val logger: Logger = LoggerFactory.getLogger(this.getClass).asInstanceOf[Logger]
    logger.addAppender(consoleAppender)
    logger.addAppender(socketAppender)
    logger.setLevel(Level.DEBUG)
    logger.setAdditive(false)
    logger.info("It works?")
    logger.detachAppender(socketAppender)
    socketAppender.stop()
    completion.set(true).delayBy(2.seconds).unsafeRunSync()
    stt.joinWithNever.unsafeRunSync()
  }
}
