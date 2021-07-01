package com.nec.cmake

import java.time.Instant
import org.apache.commons.io.FileUtils

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import scala.sys.process._
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.functions.AddPairwise
import com.nec.arrow.functions.Avg
import com.nec.arrow.functions.Sum
import com.nec.ve.VeKernelCompiler

/**
 * Utilities to build C libraries using CMake
 *
 * Major OS are supported.
 */
object CMakeBuilder {

  lazy val CLibPath = buildC(
    List(
      TransferDefinitions.TransferDefinitionsSourceCode,
      Sum.SumSourceCode,
      Avg.AvgSourceCode,
      AddPairwise.PairwiseSumCode
    ).mkString("\n \n")
  )

  lazy val CMakeListsTXT =
    s"""
cmake_minimum_required(VERSION 3.6)
project(HelloWorld LANGUAGES CXX C)
set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)
add_library(aurora4spark SHARED aurora4spark.cpp)
${VeKernelCompiler.DefaultIncludesList.map(i => s"include_directories($i)").mkString("\n")}
"""

  def buildC(cSource: String): Path = {
    val targetDir = Paths.get("target", s"c", s"${Instant.now().toEpochMilli}").toAbsolutePath
    if (Files.exists(targetDir)) {
      FileUtils.deleteDirectory(targetDir.toFile)
    }
    Files.createDirectories(targetDir)
    val tgtCl = targetDir.resolve("CMakeLists.txt")
    Files.write(tgtCl, CMakeListsTXT.getBytes("UTF-8"))
    Files.write(targetDir.resolve("aurora4spark.cpp"), cSource.getBytes("UTF-8"))
    try buildAndLink(tgtCl)
    catch {
      case e: Throwable =>
        throw new RuntimeException(s"Could not build due to $e. CMakeLists: ${CMakeListsTXT}", e)
    }
  }

  private def buildAndLink(targetPath: Path): Path = {
    val os = System.getProperty("os.name").toLowerCase

    os match {
      case _ if os.contains("win") => buildAndLinkWin(targetPath)
      case _ if os.contains("lin") => buildAndLinkLin(targetPath)
      case _                       => buildAndLinkMacos(targetPath)
    }
  }

  def runHopeOk(process: ProcessBuilder): Unit = {
    var res = ""
    var resErr = ""
    val io = new ProcessIO(
      stdin => { stdin.close() },
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

  private def buildAndLinkWin(targetPath: Path): Path = {
    val cmd = List("C:\\Program Files\\CMake\\bin\\cmake", "-A", "x64", targetPath.toString)
    val cmd2 =
      List("C:\\Program Files\\CMake\\bin\\cmake", "--build", targetPath.getParent.toString)
    runHopeOk(cmd)
    runHopeOk(cmd2)
    targetPath.getParent.resolve("Debug").resolve("aurora4spark.dll")
  }
  private def buildAndLinkMacos(targetPath: Path): Path = {
    val cmd = List("cmake", targetPath.toString)
    val cmd2 = List("make", "-C", targetPath.getParent.toString)
    runHopeOk(cmd)
    runHopeOk(cmd2)
    targetPath.getParent.resolve("libaurora4spark.dylib")
  }
  private def buildAndLinkLin(targetPath: Path): Path = {
    val cmd = List("cmake", targetPath.toString)
    val cmd2 = List("make", "-C", targetPath.getParent.toString)
    assert(cmd.! == 0)
    assert(cmd2.! == 0)
    targetPath.getParent.resolve("libaurora4spark.so")
  }
}
