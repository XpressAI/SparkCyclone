package com.nec.native.compiler

import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.util.ProcessRunner
import scala.util.Random
import java.nio.file.Paths
import java.time.Instant
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

@VectorEngineTest
final class VeCodeCompilerUnitSpec extends AnyWordSpec {
  "OnDemandVeCodeCompiler" should {
    "be able to compile well-formed C++ code using NC++" in {
      val fnName = s"func_${Random.nextInt(1000)}"
      // Define a simple function
      val code = s"""
        | #include <stdlib.h>
        |
        | extern "C" double ${fnName} (double input) {
        |   return input * 2;
        | }
        """.stripMargin

      noException should be thrownBy {
        // Initialize the compiler
        val compiler = OnDemandVeCodeCompiler(Paths.get("target", "ve", s"${Instant.now.toEpochMilli}"))

        // Compile the code and get back to .SO filepath
        val libpath1 = compiler.build(code)
        val output1 = ProcessRunner(Seq("stat", libpath1.toString), Paths.get(".")).run(true)

        // Re-compiling the same code (i.e. same hashcode) should be skipped
        val libpath2 = compiler.build(code)
        val output2 = ProcessRunner(Seq("stat", libpath2.toString), Paths.get(".")).run(true)

        // The returned library paths should be the same, and the `stat` output (touch timestamps) should be the same
        libpath1 should be (libpath2)
        output1.stdout should be (output2.stdout)
      }
    }

    // "throw an exception if NC++ compilation failed" in {
    //   // Define a function with bad syntax
    //   val code = s"""
    //     | #include <stdlib.h>
    //     |
    //     | extern "C" double func_${Random.nextInt(1000)} (double input) {
    //     |   return input *
    //     | }
    //     """.stripMargin

    //   intercept[RuntimeException] {
    //     val compiler = VeKernelCompiler(
    //       s"${getClass.getSimpleName}",
    //       Paths.get("target", "ve", s"${Instant.now.toEpochMilli}").normalize.toAbsolutePath
    //     )

    //     val libpath = compiler.compile(code)
    //   }
    // }
  }
}
