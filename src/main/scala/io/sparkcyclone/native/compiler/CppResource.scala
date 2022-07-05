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
package io.sparkcyclone.native.compiler

import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop.yarn.exceptions.ResourceNotFoundException
import java.net.URL
import java.nio.file.{Files, Path}

object CppResource {
  val CppPrefix = "io.sparkcyclone.cpp"
  val CppSourcePath = s"/${CppPrefix.replace('.', '/')}"
  val CppTargetPath = s"/cycloneve"

  lazy val Sources = {
    /*
      The `sources.bom` file is automatically generated by Make on the C++ side
      as part of the `compile` task in sbt, and contains a newline-delimited
      list of all relevant source and header files in the Cyclone C++ library.
    */
    val bomPath = s"${CppTargetPath}/sources.bom"
    val bomFile = this.getClass.getResource(bomPath)
    if (bomFile == null) {
      throw new ResourceNotFoundException(s"C++ sources BOM file not found.")
    }
    IOUtils.toString(bomFile.openStream, "UTF-8").split("\n").toSeq
  }

  final case class CppResources(all: Set[CppResource]) {
    def copyTo(destRoot: Path): Unit = {
      all.foreach(_.copyTo(destRoot))
    }
  }

  object CppResources {
    lazy val All: CppResources = CppResources({
      Sources.map { filePath =>
        CppResource(filePath, s"${CppSourcePath}/${filePath}")
      }.toSet
    })

    lazy val cycloneVeResources = Set(
      /*
        `libcyclone.so` is built by Make on the C++ side as part of the `compile`
        task in sbt (runs only if `ncc` is available in the PATH).
      */
      CppResource(name = "libcyclone.so", fullPath = s"${CppTargetPath}/libcyclone.so")
    )

    lazy val AllVe: CppResources = CppResources(cycloneVeResources ++ All.all)
  }
}

final case class CppResource(name: String, fullPath: String) {
  def resourceUrl: URL = {
    val resource = this.getClass.getResource(fullPath)
    if (resource == null) {
      throw new ResourceNotFoundException(s"Not found: ${name} // '${fullPath}'")
    }
    resource
  }

  def containingDir(inRoot: Path): Path = {
    inRoot.resolve(name).getParent
  }

  def copyTo(destRoot: Path): Unit = {
    val targetFile = destRoot.resolve(name)
    if (!Files.exists(targetFile.getParent)) {
      Files.createDirectories(targetFile.getParent)
    }

    FileUtils.copyURLToFile(resourceUrl, targetFile.toFile)
  }
}
