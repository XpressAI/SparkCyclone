package sc

import cats.effect.IO
import doobie.util.transactor.Transactor
import doobie._
import doobie.implicits._
import cats._
import cats.data._
import cats.effect.IO
import cats.implicits._
import sc.RunDatabase.ResultsInfo
import sc.RunOptions.RunResult
import scalatags.Text

import java.nio.file.{Files, Path, Paths}
import scala.collection.mutable

/**
 * Database wrapper that automatically infers the schema and adds new columns where needed
 * (so no evolutions needed when updating RunOptions and RunResult classes)
 *
 * @param transactor Doobie transactor for the SQLite database.
 */
final case class RunDatabase(transactor: Transactor[IO], uri: String) {

  private val createTable =
    sql"""
    CREATE TABLE IF NOT EXISTS run_result (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  """.update.run

  private val addFields = (RunOptions.fieldNames ++ RunResult.fieldNames)
    .map { fieldName =>
      sql"""ALTER TABLE run_result ADD COLUMN """ ++ Fragment.const(fieldName)
    }
    .map(sequel => sequel.update.run)

  def initialize: IO[Unit] =
    (createTable
      .transact(transactor) *> (addFields.traverse(_.transact(transactor).attempt).void)).void

  private def insertStmt(runOptions: RunOptions, runResult: RunResult) = {
    sql"""INSERT INTO run_result (""" ++ (RunOptions.fieldNames ++ RunResult.fieldNames).zipWithIndex
      .map {
        case (frag, 0) => Fragment.const(s"$frag")
        case (frag, _) => Fragment.const(s", $frag")
      }
      .reduce(_.combine(_)) ++ sql") VALUES ( " ++
      (runOptions.productIterator.toList ++ runResult.productIterator.toList).zipWithIndex
        .map {
          case (x: Option[_], 0) => sql"${x.map(_.toString)}"
          case (x: Option[_], _) => sql", ${x.map(_.toString)}"
          case (x, 0)            => sql"${x.toString}"
          case (x, _)            => sql", ${x.toString}"
        }
        .reduce(_.combine(_)) ++ sql")"

  }

  def insert(runOptions: RunOptions, runResult: RunResult): IO[Unit] = {
    val s = insertStmt(runOptions, runResult)
//    println(s)
    s.update.run.transact(transactor).void
  }

  import java.sql._
  def fetchResults: IO[ResultsInfo] = IO.blocking {
    val connection = DriverManager.getConnection(uri)
    try {
      val statement = connection.createStatement()
      statement.setQueryTimeout(30)
      val rs = statement.executeQuery("select * from run_result order by timestamp desc")
      val cols = (1 to rs.getMetaData.getColumnCount)
        .map(idx => rs.getMetaData.getColumnLabel(idx))
        .toList
      val buf = mutable.Buffer.empty[List[Option[AnyRef]]]
      while (rs.next()) {
        buf.append(cols.map(col => Option(rs.getObject(col))))
      }
      ResultsInfo(cols, buf.toList)
    } finally connection.close()
  }

}

object RunDatabase {

  final case class ResultsInfo(columns: List[String], data: List[List[Option[AnyRef]]]) {
    import _root_.scalatags.Text.all._
    def toTable: Text.TypedTag[String] = html(
      head(
        tag("title")("TPC Bench results"),
        raw(
          """<link rel="stylesheet" href="https://unpkg.com/purecss@2.0.6/build/pure-min.css" integrity="sha384-Uu6IeWbM+gzNVXJcM9XV3SohHtmWE+3VGi496jvgX1jyvDTXfdK+rfZc8C1Aehk5" crossorigin="anonymous">"""
        ),
        raw("""<meta name="viewport" content="width=device-width, initial-scale=1">"""),
        raw("""<style>body {font-size:0.8em; }
              |td {vertical-align:top; }
              |.failed td {
              |background: rgb(255,240,240);
              |}
              |</style>""".stripMargin)
      ),
      body(
        table(
          `class` := "pure-table-striped pure-table pure-table-horizontal",
          thead(tr(columns.map(col => th(col)))),
          tbody(data.map { row =>
            tr(
              if (row(columns.indexOf("succeeded")).contains("false")) (`class` := "failed")
              else (),
              row.zip(columns).map {
                case (None, _)                       => td()
                case (Some(value), cn @ "timestamp") => td(`class` := cn, pre(value.toString))
                case (Some(value), cn @ "gitCommitSha") =>
                  td(
                    `class` := cn,
                    pre(
                      a(
                        target := "_blank",
                        href := s"https://github.com/XpressAI/SparkCyclone/commit/${value}",
                        s"${value}"
                      )
                    )
                  )
                case (Some(value), cn) if value.toString.startsWith("http") =>
                  td(
                    `class` := cn,
                    a(
                      href := value.toString,
                      target := "_blank",
                      value.toString.replaceAllLiterally("http://", "")
                    )
                  )
                case (Some(value), cn) => td(`class` := cn, value.toString)
              }
            )
          })
        )
      )
    )

    def save: IO[Path] = IO
      .blocking {
        val absPth = Paths.get("target/tpc-html/index.html").toAbsolutePath
        Files.createDirectories(absPth.getParent)
        Files.write(absPth, toTable.render.getBytes("UTF-8"))
      }
      .flatTap(path => IO.println(s"Saved results to ${path}"))
  }
}
