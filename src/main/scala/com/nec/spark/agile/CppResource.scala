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

  val Sources = Seq(
    ("conditions_for_find.hpp", "/frovedis/core/conditions_for_find.hpp"),
    ("config.hpp", "/frovedis/core/config.hpp"),
    ("find_condition.hpp", "/frovedis/core/find_condition.hpp"),
    ("lower_bound.hpp", "/frovedis/core/lower_bound.hpp"),
    ("prefix_sum.hpp", "/frovedis/core/prefix_sum.hpp"),
    ("radix_sort.hpp", "/frovedis/core/radix_sort.hpp"),
    ("radix_sort.incl", "/frovedis/core/radix_sort.incl"),
    ("set_operations.hpp", "/frovedis/core/set_operations.hpp"),
    ("set_operations.incl1", "/frovedis/core/set_operations.incl1"),
    ("set_operations.incl2", "/frovedis/core/set_operations.incl2"),
    ("upper_bound.hpp", "/frovedis/core/upper_bound.hpp"),
    ("utility.cc", "/frovedis/core/utility.cc"),
    ("utility.hpp", "/frovedis/core/utility.hpp"),
    ("hashtable.hpp", "/frovedis/dataframe/hashtable.hpp"),
    ("join.cc", "/frovedis/dataframe/join.cc"),
    ("join.hpp", "/frovedis/dataframe/join.hpp"),
    ("char_int_conv.cc", "/frovedis/text/char_int_conv.cc"),
    ("char_int_conv.hpp", "/frovedis/text/char_int_conv.hpp"),
    ("datetime_to_words.cc", "/frovedis/text/datetime_to_words.cc"),
    ("datetime_to_words.hpp", "/frovedis/text/datetime_to_words.hpp"),
    ("datetime_utility.hpp", "/frovedis/text/datetime_utility.hpp"),
    ("dict.cc", "/frovedis/text/dict.cc"),
    ("dict.hpp", "/frovedis/text/dict.hpp"),
    ("find.cc", "/frovedis/text/find.cc"),
    ("find.hpp", "/frovedis/text/find.hpp"),
    ("float_to_words.cc", "/frovedis/text/float_to_words.cc"),
    ("float_to_words.hpp", "/frovedis/text/float_to_words.hpp"),
    ("int_to_words.cc", "/frovedis/text/int_to_words.cc"),
    ("int_to_words.hpp", "/frovedis/text/int_to_words.hpp"),
    ("parsedatetime.cc", "/frovedis/text/parsedatetime.cc"),
    ("parsedatetime.hpp", "/frovedis/text/parsedatetime.hpp"),
    ("parsefloat.cc", "/frovedis/text/parsefloat.cc"),
    ("parsefloat.hpp", "/frovedis/text/parsefloat.hpp"),
    ("parseint.hpp", "/frovedis/text/parseint.hpp"),
    ("words.cc", "/frovedis/text/words.cc"),
    ("words.hpp", "/frovedis/text/words.hpp"),
    ("cyclone.cc", "/cyclone.cc"),
    ("cyclone.hpp", "/cyclone.hpp"),
    ("Makefile", "/Makefile"),
    ("test.cpp", "/test.cpp"),
    ("transfer-definitions.hpp", "/transfer-definitions.hpp"),
    ("tuple_hash.hpp", "/tuple_hash.hpp"),
    ("utility.hpp", "/utility.hpp")
  )

  final case class CppResources(all: Set[CppResource]) {
    def copyTo(destRoot: Path): Unit = {
      all.foreach(_.copyTo(destRoot))
    }
  }

  object CppResources {
    lazy val All: CppResources = CppResources({
      Sources.map { case (fileName, filePath) =>
        CppResource(fileName, filePath)
      }.toSet
    })

    lazy val cycloneVeResources = Set(
      CppResource(name = "libcyclone.so", fullPath = s"/libcyclone.so")
    )

    lazy val AllVe: CppResources = CppResources(cycloneVeResources ++ All.all)
  }

}

final case class CppResource(name: String, fullPath: String) {
  def readString: String = IOUtils.toString(resourceUrl.openStream(), "UTF-8")
  def resourceUrl: URL = {
    try this.getClass.getResource(fullPath)
    catch {
      case npe: NullPointerException =>
        throw new ResourceNotFoundException(s"Not found: ${name} // '${fullPath}'")
    }
  }
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
