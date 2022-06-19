package com.nec.util

import java.nio.file.Paths
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

class ProcessRunnerUnitSpec extends AnyWordSpec {
  "ProcessRunner" should {
    "correctly run a shell command" in {
      val process = ProcessRunner(
        Seq("ls", "-la"),
        Paths.get("."),
        Seq(("FOO", "42"))
      )
      process.run(true)
    }
  }
}
