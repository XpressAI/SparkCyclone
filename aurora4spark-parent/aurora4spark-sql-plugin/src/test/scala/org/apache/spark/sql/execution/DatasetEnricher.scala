package org.apache.spark.sql.execution
import org.apache.spark.sql.Dataset

object DatasetEnricher {
  implicit class RichDataSetString[T](dataset: Dataset[T]) {
    def showAsString(_numRows: Int = 10, truncate: Int = 20, vertical: Boolean = false): String =
      dataset.showString(_numRows, truncate, vertical)
  }
}
