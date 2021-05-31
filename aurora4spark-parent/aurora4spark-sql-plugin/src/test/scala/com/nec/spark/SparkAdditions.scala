package com.nec.spark
import com.nec.arrow.CArrowNativeInterfaceNumeric
import com.nec.cmake.CMakeBuilder
import com.nec.spark.agile.AdditionAggregator
import com.nec.spark.agile.AggregationExpression
import com.nec.spark.agile.AggregationFunction
import com.nec.spark.agile.Aggregator
import com.nec.spark.agile.AvgAggregation
import com.nec.spark.agile.AvgAggregator
import com.nec.spark.agile.ColumnAggregator
import com.nec.spark.agile.MultipleColumnsOffHeapSubtractor
import com.nec.spark.agile.NoAggregationAggregator
import com.nec.spark.agile.SubtractExpression
import com.nec.spark.agile.SubtractionAggregator
import com.nec.spark.agile.SumAggregation
import com.nec.spark.agile.SumAggregator
import com.nec.spark.agile.SumExpression
import com.nec.spark.planning.MultipleColumnsAveragingPlanOffHeap.MultipleColumnsOffHeapAverager
import com.nec.spark.planning.MultipleColumnsSummingPlanOffHeap.MultipleColumnsOffHeapSummer
import com.nec.spark.planning.SparkSqlPlanExtension
import org.apache.log4j.Level

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.PlanExtractor.DatasetPlanExtractor
import org.apache.spark.sql.execution.SparkPlan
import org.scalatest.BeforeAndAfterAllConfigMap
import org.scalatest.ConfigMap
import org.scalatest.Informing
import org.scalatest.TestSuite

trait SparkAdditions extends BeforeAndAfterAllConfigMap {
  this: TestSuite with Informing with BeforeAndAfter =>

  protected def withSpark[T](configure: SparkConf => SparkConf)(f: SparkContext => T): T = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("local-test")
    conf.set("spark.ui.enabled", "false")
    val sparkContext = new SparkContext(configure(conf))

    try {
      f(sparkContext)
    } finally sparkContext.stop()
  }

  protected def withSparkSession[T](configure: SparkConf => SparkConf)(f: SparkSession => T): T = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("local-test")
    conf.set("spark.ui.enabled", "false")
    val sparkSession = SparkSession.builder().config(configure(conf)).getOrCreate()
    try f(sparkSession)
    finally sparkSession.stop()
  }

  protected def withSparkSession2[T](
    configure: SparkSession.Builder => SparkSession.Builder
  )(f: SparkSession => T): T = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("local-test")
    conf.set("spark.ui.enabled", "false")
    val sparkSession = configure(SparkSession.builder().config(conf)).getOrCreate()
    try f(sparkSession)
    finally sparkSession.stop()
  }

  private var debugSparkPlans: Boolean = false

  override protected def beforeAll(configMap: ConfigMap): Unit = {
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    super.beforeAll(configMap)

    this.debugSparkPlans =
      Set("1", "true").contains(configMap.getOrElse("debug.spark.plans", "0").toString.toLowerCase)
  }

  protected implicit class Richy[T](dataSet: Dataset[T]) {
    def debugConditionally(prefix: String = ""): Dataset[T] = {
      if (debugSparkPlans) {
        info(prefix + dataSet.extractQueryExecution.toString())
      }

      dataSet
    }

    def executionPlan: SparkPlan = dataSet.extractQueryExecution.executedPlan
  }

  protected def createUnsafeAggregator(aggregationFunction: AggregationFunction): Aggregator = {
    aggregationFunction match {
      case SumAggregation => new SumAggregator(new CArrowNativeInterfaceNumeric(CMakeBuilder.CLibPath.toString))
      case AvgAggregation => new AvgAggregator(new CArrowNativeInterfaceNumeric(CMakeBuilder.CLibPath.toString))
    }
  }

  protected def createUnsafeColumnAggregator(
    aggregationFunction: AggregationExpression
  ): ColumnAggregator = {
    aggregationFunction match {
      case SumExpression      => AdditionAggregator(
        new CArrowNativeInterfaceNumeric(CMakeBuilder.CLibPath.toString)
      )
      case SubtractExpression => SubtractionAggregator(MultipleColumnsOffHeapSubtractor.UnsafeBased)
      case _                  => NoAggregationAggregator
    }
  }

  after {
    SparkSqlPlanExtension.rulesToApply.clear()
  }
}
