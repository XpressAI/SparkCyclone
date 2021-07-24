package com.nec.spark.planning.simplesum
import com.nec.spark.cgescape.UnsafeExternalProcessorBase.UnsafeBatchProcessor

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}

final class JvmUnsafeSummer extends UnsafeBatchProcessor {
  private var sum: Double = 0
  def insertRow(unsafeRow: UnsafeRow): Unit = sum += unsafeRow.getDouble(0)
  def execute(): Iterator[InternalRow] = {
    val row = new UnsafeRow(1)
    val holder = new BufferHolder(row)
    val writer = new UnsafeRowWriter(holder, 1)
    holder.reset()
    writer.write(0, sum)
    Iterator(row)
  }

}
