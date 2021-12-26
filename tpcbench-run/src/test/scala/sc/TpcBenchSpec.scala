package sc

import cats.effect.unsafe.implicits.global
import org.scalatest.freespec.AnyFreeSpec
import sc.RunResults.DefaultOrdering

import java.nio.file.Files

final class TpcBenchSpec extends AnyFreeSpec {
  "name is one of the fields" in {
    assert(RunOptions.fieldNames.contains("name"))
  }

  import cats.effect.IO
  import doobie._

  val xa = Transactor
    .fromDriverManager[IO]("org.sqlite.JDBC", "jdbc:sqlite:test.db", "", "")
  val rd = RunDatabase(xa, "jdbc:sqlite:test.db")

  "it saves data into the database" in {
    rd.initialize.unsafeRunSync()

    (rd.initialize *> rd.insert(
      RunOptions.default,
      RunResult(
        succeeded = false,
        wallTime = 1,
        queryTime = "123",
        traceResults = "KK",
        appUrl = "abc",
        logOutput = (0 to 40).map(l => s"s$l").mkString("\n"),
        containerList = "http://abc\nhttp://bcd",
        metrics = "",
        finalPlan = None,
        compileTime = "abc"
      )
    )).unsafeRunSync()
  }

  "We can generate a table of results" in {
    val res = rd.fetchResults.unsafeRunSync()
    assert(res.columns.contains("aggregateOnVe"))
    assert(res.data.nonEmpty)
  }

  "We can save results into a file" in {
    val path = rd.fetchResults.map(_.reorder(DefaultOrdering)).flatMap(_.save).unsafeRunSync()
    info(s"Path saved => ${path}")
  }
}
