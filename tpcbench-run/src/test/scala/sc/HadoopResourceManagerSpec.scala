package sc

import cats.implicits.toShow
import com.eed3si9n.expecty.Expecty.expect
import org.scalatest.freespec.AnyFreeSpec
import sc.hadoop.AppsContainer

object HadoopResourceManagerSpec {}
final class HadoopResourceManagerSpec extends AnyFreeSpec {
  private def xml = scala.xml.XML.load(getClass.getResource("hadoop-apps.xml"))
  private def app = AppsContainer.parse(xml).apps.head
  "It parses" in {

    expect(app.id == "application_1638487109505_0002")
  }

  "Show shows it" in {
    expect(app.show.contains("user: "))
  }

  "App URL is generated" in {
    expect(app.appUrl == "http://cluster:8088/cluster/app/application_1638487109505_0002")
  }
}
