package com.nec.spark.executor

class TestVectorEngineExecutorLauncherControl extends SparkNecExecutorControl {
  override def init(): Unit = throw new MatchError(null)
  override def stop(): Unit = ???
}
