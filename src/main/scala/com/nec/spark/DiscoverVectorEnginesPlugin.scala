package com.nec.ve

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
  val regex = "ve[0-9]+".r
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
