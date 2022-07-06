package io.sparkcyclone.benchmarks

import cats.effect.IO
import scalatags.Text

import java.nio.file.{Files, Path, Paths}

object RunResults {
  val DefaultOrdering: List[String] =
    List(
      "id",
      "timestamp",
      "gitCommitSha",
      "gitBranch",
      "scale",
      "queryNo",
      "succeeded",
      "wallTime",
      "queryTime",
      "compileTime",
      "serializerOn",
      "logOutput",
      "appUrl",
      "containerList",
      "metrics",
      "finalPlan"
    )
}
final case class RunResults(columns: List[String], data: List[List[Option[AnyRef]]]) {

  def reorder(priorities: List[String]): RunResults = {
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
      raw(
        """<script src="https://cdnjs.cloudflare.com/ajax/libs/dialog-polyfill/0.5.6/dialog-polyfill.min.js" integrity="sha512-qUIG93zKzcLBVD5RGRbx2PBmbVRu+tJIl+EPLTus0z8I1AMru9sQYdlf6cBacSzYmZVncB9rcc8rYBnazqgrxA==" crossorigin="anonymous" referrerpolicy="no-referrer"></script>"""
      ),
      raw("""<meta name="viewport" content="width=device-width, initial-scale=1">"""),
      raw("""<style>body {font-size:14pt; }
            |td {vertical-align:top; }
            |.failed td {
            |background: rgb(255,240,240) !important;
            |}
            |td,th, button, a {
            |    white-space: nowrap;
            |}
            |tr:target td {
            |background: rgb(255,225,190) !important;
            |}
            |dialog {
            |width: 90vw
            |}
            |</style>""".stripMargin)
    ),
    body(
      table(
        `class` := "pure-table pure-table-horizontal",
        style := "font-size:9pt",
        thead(tr(columns.map(col => th(col)))),
        tbody(data.map { row =>
          val theId = row(columns.indexOf("id")).get.toString
          tr(
            id := theId,
            if (row(columns.indexOf("succeeded")).contains("false")) (`class` := "failed")
            else (),
            row.zip(columns).map {
              case (None, _) => td()
              case (Some(value), cn @ ("logOutput" | "traceResults" | "metrics" | "finalPlan"))
                  if value.toString.nonEmpty =>
                td(
                  `class` := cn,
                  tag("dialog")(pre(code(value.toString))),
                  button(
                    `onclick` := "this.parentNode.querySelector('dialog').showModal();",
                    s"View log (${value.toString.count(_ == '\n')} lines)"
                  )
                )
              case (Some(value), cn @ "containerList") if value.toString.nonEmpty =>
                val urls = value.toString.split("\n").toList
                td(
                  `class` := cn,
                  tag("dialog")(ol(urls.map(x => li(a(target := "_blank", href := x, x))))),
                  button(
                    `onclick` := "this.parentNode.querySelector('dialog').showModal();",
                    s"View ${urls.size} container URLs"
                  )
                )
              case (Some(value), cn @ "timestamp") => td(`class` := cn, pre(value.toString))
              case (Some(value), cn @ "gitBranch") =>
                td(
                  `class` := cn,
                  pre(
                    a(
                      target := "_blank",
                      href := s"https://github.com/XpressAI/SparkCyclone/commits/${value}",
                      s"${value}"
                    )
                  )
                )
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
                    if (cn == "appUrl") "Open"
                    else value.toString.replaceAllLiterally("http://", "")
                  )
                )
              case (Some(value), cn @ "id") =>
                td(`class` := cn, a(href := s"#${theId}", value.toString))
              case (Some(value), cn) => td(`class` := cn, value.toString)
            }
          )
        })
      ),
      raw(
        """<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/dialog-polyfill/0.5.6/dialog-polyfill.min.css" integrity="sha512-J2+1q+RsZuJXabBfH1q/fgRr6jMy9By5SwVLk7bScEW7NFJkMUXxfeOyyxtDe6fsaJ4jsciexSlGrPYn9YbBIg==" crossorigin="anonymous" referrerpolicy="no-referrer" />"""
      ),
      raw(
        """<script>Array.from(document.querySelectorAll('dialog')).forEach((d) => (dialogPolyfill.registerDialog(d)));</script>"""
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
