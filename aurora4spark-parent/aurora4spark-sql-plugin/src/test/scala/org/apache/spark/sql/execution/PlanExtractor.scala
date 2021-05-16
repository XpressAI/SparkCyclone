package org.apache.spark.sql.execution

import org.apache.spark.sql.Dataset

object PlanExtractor {
  implicit class DatasetPlanExtractor[T](dataSet: Dataset[T]) {
    def extractQueryExecution: QueryExecution = dataSet.queryExecution
  }
}
