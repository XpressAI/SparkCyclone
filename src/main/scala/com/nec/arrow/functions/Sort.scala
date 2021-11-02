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
package com.nec.arrow.functions

import com.nec.arrow.ArrowNativeInterface
import com.nec.arrow.ArrowNativeInterface.NativeArgument
import org.apache.arrow.vector.Float8Vector

object Sort {
  def runOn(
    nativeInterface: ArrowNativeInterface
  )(firstColumnVector: Float8Vector, outputVector: Float8Vector): Unit = {

    outputVector.setValueCount(firstColumnVector.getValueCount())

    nativeInterface.callFunctionWrapped(
      "sort_doubles",
      List(NativeArgument.input(firstColumnVector), NativeArgument.output(outputVector))
    )
  }

  def sortJVM(inputVector: Float8Vector): List[Double] =
    (0 until inputVector.getValueCount)
      .map { idx =>
        inputVector.get(idx)
      }
      .toList
      .sorted
}
