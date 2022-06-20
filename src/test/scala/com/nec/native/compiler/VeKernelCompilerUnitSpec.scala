package com.nec.native.compiler

import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.util.ProcessRunner
import scala.util.Random
import java.nio.file.Paths
import java.time.Instant
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

@VectorEngineTest
final class VeKernelCompilerUnitSpec extends AnyWordSpec {
  "VeKernelCompiler" should {
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
        val compiler = VeKernelCompiler(
          s"${getClass.getSimpleName}",
          Paths.get("target", "ve", s"${Instant.now.toEpochMilli}").normalize.toAbsolutePath
        )

        // Compile the code and get back to .SO filepath
        val libpath = compiler.compile(code)

        // Run nm on the .SO filepath to check that the function is indeed defined
        val output = ProcessRunner(Seq("nm", libpath.toString), Paths.get(".")).run(true)
        output.stdout.split("\n").find(_.contains(fnName)) should not be empty
      }
    }

    "throw an exception if NC++ compilation failed" in {
      // Define a function with bad syntax
      val code = s"""
        | #include <stdlib.h>
        |
        | extern "C" double func_${Random.nextInt(1000)} (double input) {
        |   return input *
        | }
        """.stripMargin

      intercept[RuntimeException] {
        val compiler = VeKernelCompiler(
          s"${getClass.getSimpleName}",
          Paths.get("target", "ve", s"${Instant.now.toEpochMilli}").normalize.toAbsolutePath
        )

        val libpath = compiler.compile(code)
      }
    }
  }
}
