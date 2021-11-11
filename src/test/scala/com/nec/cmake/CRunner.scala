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

import java.nio.file.Path
import com.sun.jna.Library

object CRunner {
  def runC(libPath: Path, functionName: String, args: Array[java.lang.Object]): Int = {
    // will abstract this out later
    import scala.collection.JavaConverters._
    val thingy2 =
      new Library.Handler(libPath.toString, classOf[Library], Map.empty[String, Any].asJava)
    val nl = thingy2.getNativeLibrary
    val fn = nl.getFunction(functionName)
    fn.invokeInt(args)
  }
}
