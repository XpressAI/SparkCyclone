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

/**
 * Utilities to build C libraries using CMake
 *
 * Major OS are supported.
 */
final case class CMakeBuilder(targetDir: Path) {
  def buildC(cSource: String): Path = {
    val SourcesDir = targetDir.resolve("sources")
    CppResources.All.copyTo(SourcesDir)
    lazy val CMakeListsTXT =
      s"""
cmake_minimum_required(VERSION 3.6)
project(HelloWorld LANGUAGES CXX C)
set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)
set (CMAKE_CXX_STANDARD 17)
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

object CMakeBuilder {

  def buildC(cSource: String): Path = {
    val targetDir = Paths.get("target", s"c", s"${Instant.now().toEpochMilli}").toAbsolutePath
    if (Files.exists(targetDir)) {
      FileUtils.deleteDirectory(targetDir.toFile)
    }

    Files.createDirectories(targetDir)

    CMakeBuilder(targetDir).buildC(cSource)
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
