package com.nec.spark.agile
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

/** Collect UnsafeRows, and then emit UnsafeRows */
final class UnsafeExternalDuplicator extends UnsafeBatchProcessor {
  private val rows = scala.collection.mutable.Buffer.empty[UnsafeRow]
  def insertRow(unsafeRow: UnsafeRow): Unit = rows.append(unsafeRow.copy())
  def execute(): Iterator[InternalRow] = rows.iterator
}
