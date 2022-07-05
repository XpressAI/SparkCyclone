package io.sparkcyclone.benchmarks.hadoop

import cats.Show
import org.http4s.Uri

import scala.xml.Elem

final case class AppsContainer(apps: List[AppsContainer.App])
object AppsContainer {
  def parse(xml: scala.xml.Elem): AppsContainer = AppsContainer((xml \ "app").collect {
    case app: scala.xml.Elem =>
      App.parse(app)
  }.toList)

  final case class App(
    id: String,
    user: String,
    name: String,
    queue: String,
    state: String,
    amContainerLogs: String,
    trackingUrl: String
  ) {
    def appAttemptsUrl: String =
      Uri
        .unsafeFromString(appUrl)
        .withPath(
          Uri.Path(segments =
            Vector(
              Uri.Path.Segment("ws"),
              Uri.Path.Segment("v1"),
              Uri.Path.Segment("cluster"),
              Uri.Path.Segment("apps"),
              Uri.Path.Segment(id),
              Uri.Path.Segment("appattempts")
            )
          )
        )
        .renderString

    def appUrl: String =
      Uri
        .unsafeFromString(amContainerLogs)
        .withPath(
          Uri.Path(segments =
            Vector(Uri.Path.Segment("cluster"), Uri.Path.Segment("app"), Uri.Path.Segment(id))
          )
        )
        .renderString
        .replaceAllLiterally("8042", "8088")

  }

  object App {
    implicit val showApp: Show[App] = Show.show(app =>
      List(
        "id" -> app.id,
        "user" -> app.user,
        "name" -> app.name,
        "queue" -> app.queue,
        "state" -> app.state,
        "amContainerLogs" -> app.amContainerLogs,
        "trackingUrl" -> app.trackingUrl,
        "appUrl" -> app.appUrl
      ).map { case (k, v) => s"$k: $v" }.mkString("", "\n", "\n")
    )

    def parse(app: Elem): App = {
      App(
        id = (app \ "id").text.trim,
        user = (app \ "user").text.trim,
        name = (app \ "name").text.trim,
        queue = (app \ "queue").text.trim,
        state = (app \ "state").text.trim,
        amContainerLogs = (app \ "amContainerLogs").text.trim,
        trackingUrl = (app \ "trackingUrl").text.trim
      )
    }
  }
}
