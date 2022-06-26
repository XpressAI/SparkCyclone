package com.nec.util

import scala.io.Source
import scala.sys.process.{Process, ProcessIO}
import java.nio.file.Path
import com.typesafe.scalalogging.LazyLogging

final case class ProcessOutput(stdout: String,
                               stderr: String)

final case class ProcessRunner(command: Seq[String],
                               cwd: Path,
                               env: Map[String, String] = Map.empty) extends LazyLogging {
  def run(debug: Boolean): ProcessOutput = {
    val fcwd = cwd.normalize.toAbsolutePath
    val process = Process(command, fcwd.toFile, env.toSeq: _*)

    var stdoutS = ""
    var stderrS = ""

    val procio = new ProcessIO(
      { stdin => stdin.close },
      { stdout =>
        val src = Source.fromInputStream(stdout)
        try {
          stdoutS = src.mkString
        } finally {
          stdout.close
        }
      },
      { stderr =>
        val src = Source.fromInputStream(stderr)
        try {
          stderrS = src.mkString
        } finally {
          stderr.close
        }
      }
    )

    logger.debug(s"[${hashCode.abs}] Running process for command: ${command.mkString(" ")}")
    logger.debug(s"[${hashCode.abs}] Process working directory: ${fcwd}")
    logger.debug(s"[${hashCode.abs}] Process environment: ${env}")

    val proc = process.run(procio)
    val exitcode = proc.exitValue

    if (debug) {
      logger.debug(s"[${hashCode.abs}] Process stdout:\n\n${stdoutS}\n\n")
      logger.debug(s"[${hashCode.abs}] Process stderr:\n\n${stderrS}\n\n")
    }

    assert(
      exitcode == 0,
      s"""[${hashCode.abs}] Process exited with non-zero code ${exitcode}:
          |Working directory:   ${fcwd}
          |Environment:         ${env}
          |Command:             ${command.mkString(" ")}
          |Stderr:              ${stderrS}
          |""".stripMargin
    )

    ProcessOutput(stdoutS, stderrS)
  }
}
