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
package com.nec.cmake

import com.nec.arrow.ArrowNativeInterface.NativeArgument
import com.nec.arrow.WithTestAllocator
import com.nec.native.NativeEvaluator
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.CFunction
import com.nec.spark.planning.Tracer
import com.nec.spark.planning.Tracer.Mapped
import org.scalatest.freespec.AnyFreeSpec

class TracerTest extends AnyFreeSpec {
  def includeUdp: Boolean = false
  lazy val evaluator: NativeEvaluator = NativeEvaluator.CNativeEvaluator
  "We can trace" in {
    val functionName = "test"
    val ani = evaluator.forCode(code =
      CodeLines
        .from(
          Tracer.DefineTracer.cCode,
          if (includeUdp) TcpDebug.default.headers else CodeLines.empty,
          CFunction(
            inputs = List(Tracer.TracerVector),
            outputs = Nil,
            body = CodeLines.from(
              if (includeUdp) TcpDebug.default.createSock else CodeLines.empty,
              CodeLines.debugHere,
              if (includeUdp) TcpDebug.default.close else CodeLines.empty
            )
          )
            .toCodeLinesJ(functionName)
            .cCode
        )
        .cCode
    )
    WithTestAllocator { implicit allocator =>
      val inVec = Mapped(Tracer.Launched("launchId"), "mappingId")
      val vec = inVec.createVector()
      try {
        ani.callFunctionWrapped(functionName, List(NativeArgument.input(vec)))
      } finally vec.close()
    }
  }
}
