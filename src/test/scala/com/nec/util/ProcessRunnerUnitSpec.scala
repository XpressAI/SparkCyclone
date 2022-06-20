package com.nec.util

import java.nio.file.Paths
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

class ProcessRunnerUnitSpec extends AnyWordSpec {
  "ProcessRunner" should {
    "correctly run a shell command and return stdout and stderr" in {
      val process = ProcessRunner(
        Seq("ls", "-la"),
        Paths.get("."),
        Seq(("FOO", "42"))
      )

      val output = process.run(true)

      Seq("README.md", "LICENSE", "src").foreach { token =>
        output.stdout.split("\n").find(_.endsWith(token)) should not be empty
      }
    }
  }
}
