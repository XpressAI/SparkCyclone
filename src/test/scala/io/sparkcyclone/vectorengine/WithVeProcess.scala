package io.sparkcyclone.vectorengine

import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin
import io.sparkcyclone.metrics.VeProcessMetrics
import io.sparkcyclone.data.{VeColVectorSource => VeSource}
import java.nio.file.{Path, Paths}
import com.codahale.metrics._
import org.apache.spark.SparkConf
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.resource.ResourceInformation
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait WithVeProcess extends BeforeAndAfterAll with BeforeAndAfterEach { self: Suite =>
  // TODO: Remove
  implicit val metrics0 = VeProcessMetrics.noOp

  implicit val metrics = new MetricRegistry

  /*
    Initialization is explicitly deferred to avoid creation of VeProcess when
    running tests in non-VE scope, because the instantiation of ScalaTest
    classes is eager even if annotated with @VectorEngineTest
  */
  implicit val process: VeProcess = DeferredVeProcess { () =>
    VeProcess.create(getClass.getName, 2, metrics)
  }

  implicit def source: VeSource = {
    process.source
  }

  implicit def engine: VectorEngine = {
    new VectorEngineImpl(process, metrics)
  }

  override def beforeAll: Unit = {
    super.beforeAll

    SparkCycloneExecutorPlugin.pluginContext = new PluginContext {
      def ask(message: Any): AnyRef = ???
      def conf: SparkConf = ???
      def metricRegistry = metrics
      def executorID: String = "executor-id"
      def hostname: String = "hostname"
      def resources: java.util.Map[String, ResourceInformation] = ???
      def send(message: Any): Unit = ???
    }

    SparkCycloneExecutorPlugin.veProcess = process
  }

  override def beforeEach: Unit = {
    process.load(LibCyclone.SoPath)
  }

  override def afterAll: Unit = {
    // Free all memory held by the process and close
    process.freeAll
    process.close
    super.afterAll
  }
}
