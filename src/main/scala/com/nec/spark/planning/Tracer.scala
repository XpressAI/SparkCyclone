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
package com.nec.spark.planning

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration
import com.nec.spark.agile.CFunctionGeneration.{CFunction, VeString}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{FieldVector, ValueVector, VarCharVector}
import org.apache.arrow.vector.util.Text

object Tracer {
  val TracerDefName = "TRACER"
  val TracerName = "tracer"
  val DefineTracer: CodeLines = CodeLines.from(s"#define ${TracerDefName} ${TracerName}")
  val TracerVector: CFunctionGeneration.CVector = VeString.makeCVector(TracerName)
  def includeInFunction(cFunctionNaked: CFunction): CFunction = {
    cFunctionNaked.copy(inputs = TracerVector :: cFunctionNaked.inputs)
  }

  def includeInInputs(tracer: VarCharVector, l: List[ValueVector]): List[ValueVector] = {
    tracer :: l
  }

  final case class Mapped(launched: Launched, mappingId: String) {
    import launched.launchId
    def uniqueId = s"$launchId|$mappingId"

    def createVector()(implicit bufferAllocator: BufferAllocator): VarCharVector = {
      val tracer = CFunctionGeneration.allocateFrom(TracerVector).asInstanceOf[VarCharVector]
      tracer.setValueCount(1)
      tracer.setSafe(0, new Text(s"$uniqueId"))
      tracer
    }
  }

  final case class Launched(launchId: String) {
    def map(mappingId: String): Mapped = Mapped(this, mappingId)
  }

  val TracerOutput: List[String] =
    List(
      s"std::string(${TracerDefName}->data, ${TracerDefName}->offsets[0], ${TracerDefName}->offsets[1] - ${TracerDefName}->offsets[0])"
    )

  def concatStr(parts: List[String]): String =
    parts.map(pt => s" << ${pt} ").mkString

}
