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
package com.nec.native

import com.nec.arrow.{ArrowNativeInterface, CArrowNativeInterface}
import com.nec.arrow.VeArrowNativeInterface.VeArrowNativeInterfaceLazyLib
import com.nec.native.NativeCompiler.{CNativeCompiler, CNativeCompilerDebug}
import com.typesafe.scalalogging.LazyLogging
import org.bytedeco.veoffload.veo_proc_handle

trait NativeEvaluator extends Serializable {
  def forCode(code: String): ArrowNativeInterface
}

object NativeEvaluator {

  /** Selected when running in CMake mode */
  object CNativeEvaluator extends NativeEvaluator {
    override def forCode(code: String): ArrowNativeInterface = {
      new CArrowNativeInterface(CNativeCompiler.forCode(code).toAbsolutePath.toString)
    }
  }
  final case class CNativeEvaluator(debug: Boolean) extends NativeEvaluator {
    override def forCode(code: String): ArrowNativeInterface = {
      new CArrowNativeInterface(
        (if (debug) CNativeCompilerDebug else CNativeCompiler).forCode(code).toAbsolutePath.toString
      )
    }
  }

  final class VectorEngineNativeEvaluator(proc: veo_proc_handle, nativeCompiler: NativeCompiler)
    extends NativeEvaluator
    with LazyLogging {
    override def forCode(code: String): ArrowNativeInterface = {
      val localLib = nativeCompiler.forCode(code).toString
      logger.debug(s"For evaluation, will use local lib '$localLib'")
      new VeArrowNativeInterfaceLazyLib(proc, localLib)
    }
  }

}
