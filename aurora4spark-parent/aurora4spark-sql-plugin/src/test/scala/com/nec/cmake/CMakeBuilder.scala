package com.nec.cmake

import java.time.Instant
import org.apache.commons.io.FileUtils

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import scala.sys.process._
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.functions.AddPairwise
import com.nec.arrow.functions.Avg
import com.nec.arrow.functions.Sum

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

  def buildC(cSource: String): Path = {
    val targetDir = Paths.get("target", s"c", s"${Instant.now().toEpochMilli}").toAbsolutePath
    if (Files.exists(targetDir)) {
      FileUtils.deleteDirectory(targetDir.toFile)
    }
    Files.createDirectories(targetDir)
    val tgtCl = targetDir.resolve(CMakeListsTXT.getFileName)
    Files.copy(CMakeListsTXT, tgtCl, StandardCopyOption.REPLACE_EXISTING)
    Files.write(targetDir.resolve("aurora4spark.cpp"), cSource.getBytes("UTF-8"))
    buildAndLink(tgtCl)
  }

  private def buildAndLink(targetPath: Path): Path = {
    val os = System.getProperty("os.name").toLowerCase

    os match {
      case _ if os.contains("win") => buildAndLinkWin(targetPath)
      case _ if os.contains("lin") => buildAndLinkLin(targetPath)
      case _                       => buildAndLinkMacos(targetPath)
    }
  }

  def runHopeOk(command: List[String]): Unit = {
    var res = ""
    val io = new ProcessIO(
      stdin => { stdin.close() },
      stdout => {
        val src = scala.io.Source.fromInputStream(stdout)
        try res = src.mkString
        finally stdout.close()
      },
      stderr => { stderr.close() }
    )
    val proc = command.run(io)
    assert(proc.exitValue() == 0, s"Failed; data was: $res")
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
