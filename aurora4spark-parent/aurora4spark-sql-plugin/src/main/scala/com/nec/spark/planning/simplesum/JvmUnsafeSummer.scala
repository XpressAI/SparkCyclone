package com.nec.spark.planning.simplesum
import com.nec.spark.cgescape.UnsafeExternalProcessorBase.UnsafeBatchProcessor
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter

final class JvmUnsafeSummer extends UnsafeBatchProcessor {
  private var sum: Double = 0
  def insertRow(unsafeRow: UnsafeRow): Unit = sum += unsafeRow.getDouble(0)
  def execute(): Iterator[InternalRow] = Iterator {
    val writer = new UnsafeRowWriter(1)
    writer.reset()
    writer.write(0, sum)
    writer.getRow
  }
}
