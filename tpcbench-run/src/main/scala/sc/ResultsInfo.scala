package sc

import cats.effect.IO
import scalatags.Text

import java.nio.file.{Files, Path, Paths}

object ResultsInfo {
  val DefaultOrdering: List[String] =
    List(
      "id",
      "timestamp",
      "gitCOmmitSha",
      "scale",
      "queryNo",
      "succeeded",
      "wallTime",
      "serializerOn",
      "logOutput",
      "appUrl"
    )
}
final case class ResultsInfo(columns: List[String], data: List[List[Option[AnyRef]]]) {

  def reorder(priorities: List[String]): ResultsInfo = {
    copy(
      data = data.map(dataRow =>
        columns
          .zip(dataRow)
          .sortBy(xc => {
            val p = priorities.indexOf(xc._1)
            if (p == -1) Int.MaxValue
            else p
          })
          .map(_._2)
      ),
      columns = columns.sortBy(xc => {
        val p = priorities.indexOf(xc)
        if (p == -1) Int.MaxValue
        else p
      })
    )
  }

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
            |background: rgb(255,240,240) !important;
            |}
            |tr:target td {
            |background: rgb(255,250,240) !important;
            |}
            |dialog {
            |width: 90vw
            |}
            |</style>""".stripMargin)
    ),
    body(
      table(
        `class` := "pure-table pure-table-horizontal",
        thead(tr(columns.map(col => th(col)))),
        tbody(data.map { row =>
          val theId = row(columns.indexOf("id")).get.toString
          tr(
            id := theId,
            if (row(columns.indexOf("succeeded")).contains("false")) (`class` := "failed")
            else (),
            row.zip(columns).map {
              case (None, _) => td()
              case (Some(value), cn @ "logOutput") if value.toString.nonEmpty =>
                td(
                  `class` := cn,
                  tag("dialog")(pre(code(value.toString))),
                  button(
                    `onclick` := "this.parentNode.querySelector('dialog').showModal();",
                    s"View log (${value.toString.count(_ == '\n')} lines)"
                  )
                )
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
              case (Some(value), cn @ "id") =>
                td(`class` := cn, a(href := s"#${theId}", value.toString))
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
