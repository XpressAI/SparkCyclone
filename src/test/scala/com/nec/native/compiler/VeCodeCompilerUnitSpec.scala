package com.nec.native.compiler

import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.native.NativeFunctionSamples
import com.nec.util.ProcessRunner
import scala.util.Random
import java.nio.file.Paths
import java.time.Instant
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

@VectorEngineTest
final class VeCodeCompilerUnitSpec extends AnyWordSpec {
  "OnDemandVeCodeCompiler" should {
    "be able to build an .SO from well-formed C++ code" in {
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

        // The returned library paths should be the same
        libpath1 should be (libpath2)
        // The `stat` output (touch timestamps) should be the same (i.e. the .SO file was written only once)
        output1.stdout should be (output2.stdout)
      }
    }

    "be able to build an .SO from a list of NativeFunction's" in {
      val funcs = 0.to(Random.nextInt(3)).map(_ => NativeFunctionSamples.sampleFunction)

      noException should be thrownBy {
        val compiler = OnDemandVeCodeCompiler(Paths.get("target", "ve", s"${Instant.now.toEpochMilli}"))

        // Compile the functions
        val libinfos = compiler.build(funcs)
        libinfos.keys should be (funcs.map(_.hashId).toSet)
        libinfos.values.map(_.path).toSet.size should be (1)

        // Run nm on the .SO filepath to check that the functions are indeed defined
        val output = ProcessRunner(Seq("nm", libinfos.values.head.path.toString), Paths.get(".")).run(true)
        // The names of all CFunctions defined in each NativeFunction should be found in the .SO
        funcs.flatMap(_.cfunctions).foreach { cfunc =>
          output.stdout.split("\n").find(_.contains(cfunc.name)) should not be empty
        }
      }
    }
  }
}
