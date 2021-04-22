package com.nec.spark.agile

import java.nio.file.{Files, Paths}

trait Bundle {
  def asPythonScript: Seq[String]
}

object Bundle {
  val envVars = List(
    "ASL_HOME" -> "/opt/nec/ve/nlc/2.1.0",
    "ASL_LIB_I64" -> "0",
    "ASL_LIB_MPI" -> "0",
    "LD_LIBRARY_PATH" -> "/opt/nec/ve/nlc/2.1.0/lib",
    "NCC_INCLUDE_PATH" -> "/opt/nec/ve/nlc/2.1.0/include/inc:/opt/nec/ve/nlc/2.1.0/include",
    "NFORT_INCLUDE_PATH" -> "/opt/nec/ve/nlc/2.1.0/include/mod:/opt/nec/ve/nlc/2.1.0/include",
    "NLC_HOME" -> "/opt/nec/ve/nlc/2.1.0",
    "NLC_LIB_I64" -> "0",
    "NLC_LIB_MPI" -> "0",
    "NLC_VERSION" -> "2.1.0",
    "VE_LD_LIBRARY_PATH" -> "/opt/nec/ve/nlc/2.1.0/lib",
    "VE_LIBRARY_PATH" -> "/opt/nec/ve/nlc/2.1.0/lib",
    "VE_NLC_STATIC_LIBRARY_PATH" -> "/opt/nec/ve/nlc/2.1.0/lib"
  )

  def envs: String = envVars
    .map { case (k, v) =>
      s"""os.environ["${k}"] = "${v}""""
    }
    .toList
    .mkString("\n")

  def sumBigDecimalsPurePython(nums: List[BigDecimal]): Bundle = new Bundle {
    override def asPythonScript: Seq[String] = {
      Seq(s"""
             |
             |import os
             |
             |${envs}
             |
             |import nlcpy
             |import sys
             |numbers = [${nums.map(_.toBigInt().toString()).mkString(", ")}]
             |print(int(nlcpy.sum(numbers)))
             |""".stripMargin)
    }
  }

  def sumBigDecimals(numbers: List[BigDecimal]): Bundle = new Bundle {
    // todo use actual numbers
    def asPythonScript: Seq[String] = {
      val script = new String(Files.readAllBytes(Paths.get(getClass.getResource("/sum.py").toURI)))

      Seq(script) ++ numbers.map(_.toString())
    }
  }
}
