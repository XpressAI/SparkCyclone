package com.nec.ve

import com.nec.spark.SparkAdditions
import com.nec.tpc.TPCHVESqlSpec
import com.nec.ve.CachingSpec.SampleStructure
import org.scalatest.freespec.AnyFreeSpec

object CachingSpec {
  final case class SampleStructure(str: String, num: Double)
  val SampleItems = List(
    SampleStructure(str = "x", num = 5),
    SampleStructure(str = "yz", num = 6),
    SampleStructure(str = "ab", num = 5),
    SampleStructure(str = "cd", num = 5)
  )
}
final class CachingSpec extends AnyFreeSpec with SparkAdditions with VeKernelInfra {
  "We can retrieve cached items back out" in withSparkSession2(
    TPCHVESqlSpec.VeConfiguration(failFast = true)
  ) { sparkSession =>
    import sparkSession.implicits._
    sparkSession
      .createDataset(CachingSpec.SampleItems)
      .repartition(2)
      .createTempView("sample")

    sparkSession.sql("cache table sample")

    val query = sparkSession.sql("select * from sample where num = 5").as[SampleStructure]
    val plan = query.queryExecution.executedPlan.toString()
    info(plan)
    val rddInfo = query.queryExecution.executedPlan.execute()
    info(s"$rddInfo")
    val result = query.collect().toList.toSet
    val expectedResult = CachingSpec.SampleItems.filter(_.num == 5).toSet
    assert(result == expectedResult, "results should be the same")

    assert(plan.contains("VeFetchFromCachePlan"), "info on ve fetch from cache should be there")
  }

}
