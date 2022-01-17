package com.nec.ve

import com.typesafe.scalalogging.LazyLogging

import scala.sys.process.{ProcessBuilder, ProcessIO}

object ProcessRunner extends LazyLogging {
  def runHopeOk(process: ProcessBuilder, doDebug: Boolean): Unit = {
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
    val ev = proc.exitValue()
    if (doDebug) {
      logger.debug(s"NCC output: \n${res}; \n${resErr}")
    }
    assert(ev == 0, s"Failed; data was: $res; process was ${process}; $resErr")
  }

}
