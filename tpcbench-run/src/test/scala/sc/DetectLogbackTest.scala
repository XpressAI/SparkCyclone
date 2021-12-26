package sc

import com.eed3si9n.expecty.Expecty
import org.scalatest.freespec.AnyFreeSpec

final class DetectLogbackTest extends AnyFreeSpec {
  "It finds logback-classic" in {
    Expecty.assert(DetectLogback.LogbackItemsClasspath.exists(_.contains("logback-classic")))
  }
  "It finds logback-core" in {
    Expecty.assert(DetectLogback.LogbackItemsClasspath.exists(_.contains("logback-core")))
  }
  "It finds log4j-over-slf4j" in {
    Expecty.assert(DetectLogback.LogbackItemsClasspath.exists(_.contains("log4j-over-slf4j")))
  }
  "It finds nothing else" in {
    Expecty.assert(DetectLogback.LogbackItemsClasspath.size == 3)
  }
}
