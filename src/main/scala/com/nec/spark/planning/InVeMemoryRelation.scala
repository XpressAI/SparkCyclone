//package com.nec.spark.planning
//
//import org.apache.spark.sql.catalyst.expressions.Attribute
//
//final case class InVeMemoryRelation(output: Seq[Attribute])
//  extends org.apache.spark.sql.catalyst.plans.logical.LeafNode {
//
//  override def innerChildren: Seq[SparkPlan] = Seq(cachedPlan)
//
//  override def doCanonicalize(): logical.LogicalPlan =
//    copy(output = output.map(QueryPlan.normalizeExpressions(_, cachedPlan.output)),
//      cacheBuilder,
//      outputOrdering)
//
//  @transient val partitionStatistics = new PartitionStatistics(output)
//
//  def cachedPlan: SparkPlan = cacheBuilder.cachedPlan
//
//  override def computeStats(): Statistics = {
//    if (!cacheBuilder.isCachedColumnBuffersLoaded) {
//      // Underlying columnar RDD hasn't been materialized, use the stats from the plan to cache.
//      statsOfPlanToCache
//    } else {
//      statsOfPlanToCache.copy(
//        sizeInBytes = cacheBuilder.sizeInBytesStats.value.longValue,
//        rowCount = Some(cacheBuilder.rowCountStats.value.longValue)
//      )
//    }
//  }
//
//  def withOutput(newOutput: Seq[Attribute]): InMemoryRelation =
//    InMemoryRelation(newOutput, cacheBuilder, outputOrdering, statsOfPlanToCache)
//
//  override def newInstance(): this.type = {
//    InMemoryRelation(
//      output.map(_.newInstance()),
//      cacheBuilder,
//      outputOrdering,
//      statsOfPlanToCache).asInstanceOf[this.type]
//  }
//
//  // override `clone` since the default implementation won't carry over mutable states.
//  override def clone(): LogicalPlan = {
//    val cloned = this.copy()
//    cloned.statsOfPlanToCache = this.statsOfPlanToCache
//    cloned
//  }
//
//  override def simpleString(maxFields: Int): String =
//    s"InMemoryRelation [${truncatedString(output, ", ", maxFields)}], ${cacheBuilder.storageLevel}"
//}
