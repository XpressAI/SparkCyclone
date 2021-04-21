package com.nec.spark.agile

import java.nio.file.{Files, Paths}

trait Bundle {
  def asPythonScript: String
}

object Bundle {
  def sumBigDecimals(numbers: List[BigDecimal]): Bundle = new Bundle {
    // todo use actual numbers
    def asPythonScript: String = new String(
      Files.readAllBytes(Paths.get(getClass.getResource("/sum-with-kernel.py").toURI))
    )
  }
}
