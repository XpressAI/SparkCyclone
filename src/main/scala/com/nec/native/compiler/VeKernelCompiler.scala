/*
 * Copyright (c) 2022 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.nec.native.compiler

import com.nec.util.ProcessRunner
import scala.collection.JavaConverters._
import java.nio.file.{Files, Path}
import java.nio.file.attribute.{PosixFilePermission, PosixFilePermissions}
import com.typesafe.scalalogging.LazyLogging

object VeKernelCompiler {
  final val FileAttributes = {
    val permissions = Set[PosixFilePermission](
      PosixFilePermission.OWNER_READ,
      PosixFilePermission.OWNER_WRITE,
      PosixFilePermission.OWNER_EXECUTE,
      PosixFilePermission.GROUP_READ,
      PosixFilePermission.GROUP_EXECUTE,
      PosixFilePermission.OTHERS_READ,
      PosixFilePermission.OTHERS_EXECUTE
    )
    PosixFilePermissions.asFileAttribute(permissions.asJava)
  }

  final val PrefixPattern = "[a-zA-Z0-9_.-]+".r.pattern
}

final case class VeKernelCompiler(prefix: String,
                                  buildDir: Path,
                                  config: VeCompilerConfig = VeCompilerConfig.defaults) extends LazyLogging {
  require(VeKernelCompiler.PrefixPattern.matcher(prefix).matches, s"Prefix must match the following regex: ${VeKernelCompiler.PrefixPattern}")
  require(buildDir.normalize.toAbsolutePath == buildDir, "Target build directory should be a normalized absolute path")

  def compile(code: String): Path = {
    // Create the build directory if not existent
    if (!Files.exists(buildDir)) {
      Files.createDirectories(buildDir, VeKernelCompiler.FileAttributes)
    }

    // Copy libcyclone sources over to the build directory
    val sourcesDir = buildDir.resolve("sources")
    CppResource.CppResources.AllVe.copyTo(sourcesDir)

    // Accumulate the set of libcyclone includes
    val includes = CppResource.CppResources.AllVe.all
      .map(_.containingDir(sourcesDir))
      .map(i => i.toUri.toString.drop(sourcesDir.getParent.toUri.toString.length))

    // Accumulate the set of pre-compiled libraries
    val libraries = CppResource.CppResources.AllVe.all
      .filter(_.name.endsWith(".so"))
    assert(libraries.nonEmpty, s"Expected to have at least 1 .so file, found nond. Source: ${CppResource.CppResources.AllVe}")

    // Source, Object, and SO filepaths
    val cFile = buildDir.resolve(s"${prefix}.c")
    val oFile = buildDir.resolve(s"${prefix}.o")
    val soFile = buildDir.resolve(s"${prefix}.so")

    // Construct the compilation command
    val command1 = Seq(config.nccPath) ++
      config.compilerFlags ++
      includes.map(x => s"-I${x}") ++
      Seq(
        "-Isources/",
        "-c",
        cFile.toString,
        "-o",
        oFile.toString
      )

    // Construct the linking command
    val command2 = Seq(config.nccPath, "-shared", "-pthread") ++
      Seq(
        "-o",
        soFile.toString,
        oFile.toString
      ) ++
      libraries.map { lib => sourcesDir.resolve(lib.name).toString }

    try {
      // Write out the source code to file
      Files.write(cFile, code.getBytes)

      // Compile the source file
      logger.info(s"Compilation command:  ${command1}")
      ProcessRunner(command1, buildDir).run(config.doDebug)

      // Update file permissions for the object file
      ProcessRunner(Seq("chmod", "777", oFile.toString), buildDir).run(config.doDebug)

      // Link the object file to build .SO
      logger.info(s"Linking command:  ${command2}")
      ProcessRunner(command2, buildDir).run(config.doDebug)

      // Return the .SO filepath
      soFile

    } catch {
      case e: Throwable =>
        throw new RuntimeException(s"C++ source compilation for ${cFile} failed: ${e}", e)
    }
  }
}
