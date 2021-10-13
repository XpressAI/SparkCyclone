package com.nec.cmake.functions

import com.nec.cmake.{CMakeBuilder, UdpDebug}
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import org.scalatest.freespec.AnyFreeSpec

final class IncludeUDPDebugSpec extends AnyFreeSpec {
  "It works" in {
    if (!scala.util.Properties.isWin) {
      val debugger = UdpDebug.Always("sock", "dest", "127.0.0.1", "12345")
      val code = CodeLines.from(
        debugger.headers,
        s"void do() {",
        CodeLines.from(debugger.createSock, debugger.send("\"test\""), debugger.close).indented,
        "}"
      )

      CMakeBuilder.buildC(code.cCode, debug = true)
    }
  }
}
