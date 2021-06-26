package com.nec.spark.cgescape

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

trait UnsafeBatchProcessor {
  def insertRow(unsafeRow: UnsafeRow): Unit
  def execute(): Iterator[InternalRow]
}
