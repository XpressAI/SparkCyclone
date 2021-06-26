package com.nec.spark.agile
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

trait UnsafeBatchProcessor {
  def insertRow(unsafeRow: UnsafeRow): Unit
  def execute(): Iterator[InternalRow]
}
