package sc

import cats.effect.unsafe.implicits.global
import org.scalatest.freespec.AnyFreeSpec
import sc.RunOptions.{Log4jFile, RunResult}

import java.nio.file.Files

final class TpcBenchSpec extends AnyFreeSpec {
  "log4j file is retrieved" in {
    assert(Files.exists(Log4jFile))
  }
  "name is one of the fields" in {
    assert(RunOptions.fieldNames.contains("name"))
  }

  "it saves data into the database" in {
    import doobie._
    import doobie.implicits._
    import cats._
    import cats.data._
    import cats.effect.IO
    import cats.implicits._

    val xa = Transactor
      .fromDriverManager[IO]("org.sqlite.JDBC", "jdbc:sqlite:test.db", "", "")

    val rd = RunDatabase(xa)
    (rd.initialize).unsafeRunSync()

    (rd.initialize *> rd.insert(
      RunOptions.default,
      RunResult(
        succeeded = true,
        wallTime = 1,
        queryTime = 123,
        traceResults = "KK",
        appUrl = "abc"
      )
    )).unsafeRunSync()
  }
}
