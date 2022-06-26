package com.nec.native

import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.native.compiler.OnDemandVeCodeCompiler
import com.nec.util.ProcessRunner
import scala.util.Random
import java.nio.file.Paths
import java.time.Instant
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

@VectorEngineTest
final class CachingNativeCodeCompilerUnitSpec extends AnyWordSpec {
  "CachingNativeCodeCompiler" should {
    "be able to cache .SO builds of NativeFunction's" in {
      val func1 = NativeFunctionSamples.sampleFunction
      val func2 = NativeFunctionSamples.sampleFunction
      val func3 = NativeFunctionSamples.sampleFunction
      val fgroup1 = Seq(func1, func2)
      val fgroup2 = Seq(func2, func3)

      noException should be thrownBy {
        val cwd = Paths.get("target", "ve", s"${Instant.now.toEpochMilli}")

        // Create the caching compiler
        val compiler1 = CachingNativeCodeCompiler(OnDemandVeCodeCompiler(cwd))

        // Compile func1 and func2 together
        val libpaths1 = compiler1.build(fgroup1)
        libpaths1.keys should be (fgroup1.map(_.hashId).toSet)
        // Only one library should be returned
        libpaths1.values.toSet.size should be (1)

        // Compile func2 and func3 together - func2 should be already cached from the first compilation
        val libpaths2 = compiler1.build(fgroup2)
        libpaths2.keys should be (fgroup2.map(_.hashId).toSet)
        // Two libraries should be returned - one for func2 (cached) and one for func3 (new)
        libpaths2.values.toSet.size should be (2)

        // The paths returned by the first compilation should be a subset of those returned by the second compilation
        libpaths1.values.toSet.subsetOf(libpaths2.values.toSet) should be (true)

        val path1 = libpaths1.values.head.path
        val path2 = (libpaths2.values.toSet -- libpaths1.values.toSet).head.path

        // Run nm on the .SO filepath to check that the functions are indeed defined
        val output1 = ProcessRunner(Seq("nm", path1.toString), Paths.get(".")).run(true).stdout.split("\n")
        val output2 = ProcessRunner(Seq("nm", path2.toString), Paths.get(".")).run(true).stdout.split("\n")

        // The functions should be defined in the libraries that they were first compiled to
        Seq(
          (func1, true),
          (func2, true),
          (func3, false)
        ).foreach { case (func, expected) =>
          output1.find(_.contains(func.name)).nonEmpty should be (expected)
          output2.find(_.contains(func.name)).nonEmpty should be (! expected)
        }

        // Create a second instance of the caching compiler with the same build directory
        val compiler2 = CachingNativeCodeCompiler(OnDemandVeCodeCompiler(cwd))

        // The cache should be rebuilt from the indices written out to the build directory
        compiler2.buildcache.keys should be (Set(func1, func2, func3).map(_.hashId))

        // Building with the second instance of the compiler should hit the cache
        compiler2.build(fgroup1) should be (libpaths1)
        compiler2.build(fgroup2) should be (libpaths2)
      }
    }
  }
}
