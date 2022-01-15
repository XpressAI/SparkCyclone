package sc

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.net.SocketAppender
import ch.qos.logback.core.ConsoleAppender
import com.comcast.ip4s.Host
import fs2.concurrent.SignallingRef
import fs2.io.net.Network
import org.scalatest.freespec.AnyFreeSpec
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt

final class LogbackOverSocketTest extends AnyFreeSpec {
  "It works" ignore {
    import ch.qos.logback.classic.encoder.PatternLayoutEncoder
    import ch.qos.logback.classic.spi.ILoggingEvent
    import ch.qos.logback.classic.{Logger, LoggerContext}

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

    Network[IO]
      .serverResource(address = Host.fromString("127.0.0.1"))
      .use { case (socketAddress, socketStream) =>
        val sr = socketStream
          .flatMap(socket => LogbackListener.getLoggingEvents(socket))
          .interruptWhen(haltWhenTrue = completion)
          .compile
          .toList

        val stt = sr.start.unsafeRunSync()

        val socketAppender = new SocketAppender()
        socketAppender.setContext(lc)
        socketAppender.setRemoteHost("127.0.0.1")
        socketAppender.setPort(socketAddress.port.value)
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
        val res = stt.joinWithNever.unsafeRunSync()
        val messages = res.map(_.getMessage)
        IO.delay(assert(messages.contains("It works?")))
      }
      .unsafeRunSync()

  }
}
