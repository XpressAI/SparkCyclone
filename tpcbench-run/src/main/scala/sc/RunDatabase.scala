package sc

import cats.effect.IO
import doobie.util.transactor.Transactor
import doobie._
import doobie.implicits._
import cats._
import cats.data._
import cats.effect.IO
import cats.implicits._
import sc.RunOptions.RunResult

/**
 * Database wrapper that automatically infers the schema and adds new columns where needed
 * (so no evolutions needed when updating RunOptions and RunResult classes)
 *
 * @param transactor Doobie transactor for the SQLite database.
 */
final case class RunDatabase(transactor: Transactor[IO]) {

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

}
