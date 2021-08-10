package com.nec.agent
import com.nec.spark.executor.SparkNecExecutorControl
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers._

final class ServiceLoaderCheck extends AnyFreeSpec {
  "We can load the test based service loader" in {
    a[MatchError] shouldBe thrownBy { SparkNecExecutorControl.getInstance().init() }
  }
}
