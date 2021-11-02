/*
 * Copyright (c) 2021 Xpress AI.
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
package com.nec.cmake

import java.time.Instant
import org.apache.commons.io.FileUtils

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import scala.sys.process._
import com.nec.arrow.TransferDefinitions
import com.nec.cmake.CMakeBuilder.Builder
import com.nec.spark.agile.CppResource.CppResources
import com.typesafe.scalalogging.LazyLogging
import javassist.compiler.CompileError

/**
 * Utilities to build C libraries using CMake
 *
 * Major OS are supported.
 */
final case class CMakeBuilder(targetDir: Path, debug: Boolean) {
  def buildC(cSource: String): Path = {
    val SourcesDir = targetDir.resolve("sources")
    CppResources.All.copyTo(SourcesDir)
    val maybeDebug = if (debug) "add_definitions(-DDEBUG)" else ""
    lazy val CMakeListsTXT =
      s"""
cmake_minimum_required(VERSION 3.6)
project(HelloWorld LANGUAGES CXX C)
set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)
set (CMAKE_CXX_STANDARD 17)
$maybeDebug
${CppResources.All.all
        .map(_.containingDir(SourcesDir))
        .toList
        .map(i =>
          s"include_directories(${i.toUri.toString.drop(SourcesDir.getParent.toUri.toString.length)})"
        )
        .mkString("\n")}

add_library(aurora4spark SHARED aurora4spark.cpp)
if(WIN32)
  target_link_libraries(aurora4spark wsock32 ws2_32)
endif()
"""

    val tgtCl = targetDir.resolve("CMakeLists.txt")
    Files.write(tgtCl, CMakeListsTXT.getBytes("UTF-8"))
    Files.write(targetDir.resolve("aurora4spark.cpp"), cSource.getBytes("UTF-8"))
    try Builder.default.buildAndLink(tgtCl)
    catch {
      case e: Throwable =>
        throw new RuntimeException(
          s"Could not build due to $e. CMakeLists: ${CMakeListsTXT}; source was '\n${cSource}\n'",
          e
        )
    }
  }
}

object CMakeBuilder extends LazyLogging {

  def buildC(cSource: String, debug: Boolean = false): Path = {
    val targetDir = Paths.get("target", s"c", s"${Instant.now().toEpochMilli}").toAbsolutePath
    if (Files.exists(targetDir)) {
      FileUtils.deleteDirectory(targetDir.toFile)
    }

    Files.createDirectories(targetDir)

    CMakeBuilder(targetDir, debug).buildC(cSource)
  }

  def buildCLogging(cSource: String, debug: Boolean = false): Path = {
    try {
      logger.debug(s"Code to compile: ${cSource}")
      buildC(cSource, debug)
    } catch {
      case e: Exception =>
        logger.info(s"Could not compile code due to error: ${e}", e)
        e.toString
          .split("\r?\n")
          .find(_.contains(": error "))
          .foreach(errLine => logger.error(errLine))
        throw new CompileError("Could not compile code. Reported in the log.")
    }
  }

  sealed trait Builder {
    def prepare(targetPath: Path): Unit

    def buildLibrary(targetPath: Path): Path

    final def buildAndLink(targetPath: Path): Path = {
      prepare(targetPath)
      buildLibrary(targetPath)
    }
  }

  object Builder {
    def default: Builder = {
      val os = System.getProperty("os.name").toLowerCase
      os match {
        case _ if os.contains("win") => WindowsBuilder
        case _ if os.contains("lin") => LinuxBuilder
        case _                       => MacOSBuilder
      }
    }

    object WindowsBuilder extends Builder {
      override def prepare(targetPath: Path): Unit = {
        val cmd = List("C:\\Program Files\\CMake\\bin\\cmake", "-A", "x64", targetPath.toString)
        runHopeOk(cmd)
      }

      override def buildLibrary(targetPath: Path): Path = {
        val cmd2 =
          List("C:\\Program Files\\CMake\\bin\\cmake", "--build", targetPath.getParent.toString)
        runHopeOk(cmd2)
        targetPath.getParent.resolve("Debug").resolve("aurora4spark.dll")
      }
    }

    object LinuxBuilder extends Builder {
      override def prepare(targetPath: Path): Unit = {
        val cmd = List("cmake", targetPath.toString)
        runHopeOk(cmd)
      }

      override def buildLibrary(targetPath: Path): Path = {
        val cmd2 = List("make", "-C", targetPath.getParent.toString)
        runHopeOk(cmd2)
        targetPath.getParent.resolve("libaurora4spark.so")
      }
    }

    object MacOSBuilder extends Builder {
      override def prepare(targetPath: Path): Unit = {
        val cmd = List("cmake", targetPath.toString)
        runHopeOk(cmd)
      }

      override def buildLibrary(targetPath: Path): Path = {
        val cmd2 = List("make", "-C", targetPath.getParent.toString)
        runHopeOk(cmd2)
        targetPath.getParent.resolve("libaurora4spark.dylib")
      }
    }

    private def runHopeOk(process: ProcessBuilder): Unit = {
      var res = ""
      var resErr = ""
      val io = new ProcessIO(
        stdin => stdin.close(),
        stdout => {
          val src = scala.io.Source.fromInputStream(stdout)
          try res = src.mkString
          finally stdout.close()
        },
        stderr => {
          val src = scala.io.Source.fromInputStream(stderr)
          try resErr = src.mkString
          finally stderr.close()
        }
      )
      val proc = process.run(io)
      assert(proc.exitValue() == 0, s"Failed; data was: $res; process was ${process}; $resErr")
    }
  }

}
