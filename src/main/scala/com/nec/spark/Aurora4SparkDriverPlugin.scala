package com.nec.spark

import com.nec.spark.planning.CEvaluationPlan.HasFloat8Vector.RichObject
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSessionExtensions

object Aurora4SparkDriverPlugin {

  private val wasAddedTo = scala.collection.mutable.Set.empty[SparkSessionExtensions]

  def injectVeoExtension(sparkSessionExtensions: SparkSessionExtensions): Unit =
    if (!wasAddedTo.contains(sparkSessionExtensions))
      new LocalVeoExtension().apply(sparkSessionExtensions)

  injectVeoExtension {
    SparkSession.getActiveSession
      .orElse(SparkSession.getDefaultSession)
      .get
      .readPrivate
      .get[SparkSessionExtensions]("extensions")
  }
}
