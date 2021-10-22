package com.nec.cmake.functions

import com.nec.cmake.{CMakeBuilder, TcpDebug}
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import org.scalatest.freespec.AnyFreeSpec

final class IncludeTCPDebugSpec extends AnyFreeSpec {
  "It works" in {
    if (!scala.util.Properties.isWin) {
      val debugger = TcpDebug.Always("sock", "dest", "\"127.0.0.1\"", "12345")
      val code = CodeLines.from(
        debugger.headers,
        s"void debugIt() {",
        CodeLines.from(debugger.createSock, debugger.send("\"test\""), debugger.close).indented,
        "}"
      )

      println(code.cCode)

      CMakeBuilder.buildC(code.cCode, debug = true)
    }
  }
}
