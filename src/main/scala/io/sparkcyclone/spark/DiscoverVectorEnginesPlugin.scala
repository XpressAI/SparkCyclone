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
package io.sparkcyclone.ve

import java.nio.file.Files
import java.nio.file.Paths
import org.apache.spark.api.resource.ResourceDiscoveryPlugin
import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceRequest
import org.apache.spark.SparkConf
import java.util.Optional
import org.apache.spark.resource.ResourceInformation
import org.apache.hadoop.yarn.api.records

object DiscoverVectorEnginesPlugin {
  val regex = "^ve[0-7]$".r
  def detectVE(): List[String] = {
    import scala.collection.JavaConverters._
    Files
      .list(Paths.get("/dev/"))
      .iterator()
      .asScala
      .filter(path => regex.unapplySeq(path.getFileName().toString()).nonEmpty)
      .map(_.toString.drop(7))
      .toList
      .sorted
  }
}

class DiscoverVectorEnginesPlugin extends ResourceDiscoveryPlugin with Logging {

  override def discoverResource(
    request: ResourceRequest,
    conf: SparkConf
  ): Optional[ResourceInformation] = {
    if (request.id.resourceName == "ve") {
      logInfo(s"Requested ${request.amount} Vector Engines...")
      val foundVEs = DiscoverVectorEnginesPlugin.detectVE()
      if (request.amount > foundVEs.size) {
        logError(s"Only found ${foundVEs.size} Vector Engines - requested ${request.amount}")
      }
      Optional.of(new ResourceInformation("ve", foundVEs.toArray.sorted.take(request.amount.toInt)))
    } else Optional.empty()
  }

}
