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
