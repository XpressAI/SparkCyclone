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
package com.nec.spark.agile

import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop.yarn.exceptions.ResourceNotFoundException

import java.net.URL
import java.nio.file.{Files, Path}

object CppResource {
  val CppPrefix = "com.nec.cyclone.cpp"

  val CppPrefixPath: String = CppPrefix.replace('.', '/')

  val Sources = Seq(
    "frovedis/core/conditions_for_find.hpp",
    "frovedis/core/config.hpp",
    "frovedis/core/find_condition.hpp",
    "frovedis/core/lower_bound.hpp",
    "frovedis/core/prefix_sum.hpp",
    "frovedis/core/radix_sort.hpp",
    "frovedis/core/radix_sort.incl",
    "frovedis/core/set_operations.hpp",
    "frovedis/core/set_operations.incl1",
    "frovedis/core/set_operations.incl2",
    "frovedis/core/upper_bound.hpp",
    "frovedis/core/utility.cc",
    "frovedis/core/utility.hpp",
    "frovedis/dataframe/hashtable.hpp",
    "frovedis/dataframe/join.cc",
    "frovedis/dataframe/join.hpp",
    "frovedis/text/char_int_conv.cc",
    "frovedis/text/char_int_conv.hpp",
    "frovedis/text/datetime_to_words.cc",
    "frovedis/text/datetime_to_words.hpp",
    "frovedis/text/datetime_utility.hpp",
    "frovedis/text/dict.cc",
    "frovedis/text/dict.hpp",
    "frovedis/text/find.cc",
    "frovedis/text/find.hpp",
    "frovedis/text/float_to_words.cc",
    "frovedis/text/float_to_words.hpp",
    "frovedis/text/int_to_words.cc",
    "frovedis/text/int_to_words.hpp",
    "frovedis/text/parsedatetime.cc",
    "frovedis/text/parsedatetime.hpp",
    "frovedis/text/parsefloat.cc",
    "frovedis/text/parsefloat.hpp",
    "frovedis/text/parseint.hpp",
    "frovedis/text/words.cc",
    "frovedis/text/words.hpp",
    "cyclone/cyclone.cc",
    "cyclone/cyclone.hpp",
    "cyclone/nullable_scalar_vector.cc",
    "cyclone/nullable_varchar_vector.cc",
    "cyclone/transfer-definitions.hpp",
    "cyclone/tuple_hash.hpp",
    "Makefile",
  )

  final case class CppResources(all: Set[CppResource]) {
    def copyTo(destRoot: Path): Unit = {
      all.foreach(_.copyTo(destRoot))
    }
  }

  object CppResources {
    lazy val All: CppResources = CppResources({
      Sources.map { filePath =>
        CppResource(filePath, "/" + CppPrefixPath + "/" + filePath)
      }.toSet
    })

    lazy val cycloneVeResources = Set(
      CppResource(name = "libcyclone.so", fullPath = s"/cycloneve/libcyclone.so")
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

  def readString: String = IOUtils.toString(resourceUrl.openStream(), "UTF-8")

  def resourceFile(inRoot: Path): Path = inRoot.resolve(name)

  def containingDir(inRoot: Path): Path = resourceFile(inRoot).getParent

  def copyTo(destRoot: Path): Unit = {
    val targetFile = resourceFile(destRoot)
    if (!Files.exists(targetFile.getParent)) {
      Files.createDirectories(targetFile.getParent)
    }

    FileUtils.copyURLToFile(resourceUrl, targetFile.toFile)
  }
}
