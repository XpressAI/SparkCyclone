package sc

import cats.implicits.toShow
import com.eed3si9n.expecty.Expecty.expect
import org.scalatest.freespec.AnyFreeSpec
import sc.hadoop.{AppAttempt, AppAttemptContainer, AppsContainer}

import scala.xml.Elem

object HadoopResourceManagerSpec {}
final class HadoopResourceManagerSpec extends AnyFreeSpec {
  private def xml: Elem = scala.xml.XML.load(getClass.getResource("hadoop-apps.xml"))
  private def app: AppsContainer.App = AppsContainer.parse(xml).apps.head
  "It parses" in {

    expect(app.id == "application_1638487109505_0002")
  }

  "Show shows it" in {
    expect(app.show.contains("user: "))
  }

  "App URL is generated" in {
    expect(app.appUrl == "http://cluster:8088/cluster/app/application_1638487109505_0002")
  }
  "Attempts URL is generated" in {
    expect(
      app.appAttemptsUrl == "http://cluster:8088/ws/v1/cluster/apps/application_1638487109505_0002/appattempts"
    )
  }

  private def xmlAppAttempts: Elem =
    scala.xml.XML.load(getClass.getResource("hadoop-appattempts.xml"))

  private def xmlContainers: Elem =
    scala.xml.XML.load(getClass.getResource("hadoop-appattempt-containers.xml"))

  "We extract the app attempt" in {
    val attempts = AppAttempt.listFromXml(xmlAppAttempts)
    val ids = attempts.map(_.appAttemptId)
    assert(ids == List("appattempt_1638487109505_0440_000001"))
  }

  "We extract App container log urls" in {
    val list = AppAttemptContainer.listFromXml(xmlContainers)
    val urls = list.map(_.logUrl)
    assert(
      urls.contains(
        "http://the-server:8042/node/containerlogs/container_1638487109505_0440_01_000001/github"
      )
    )
  }
}
