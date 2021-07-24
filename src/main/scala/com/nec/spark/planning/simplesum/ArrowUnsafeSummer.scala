package com.nec.spark.planning.simplesum
import org.apache.spark.sql.catalyst.InternalRow
import com.nec.spark.cgescape.UnsafeExternalProcessorBase.UnsafeBatchProcessor
import com.nec.spark.planning.ArrowSummingPlan.ArrowSummer
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.VectorSchemaRoot

import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

/** Container to aggregate all the data coming in and place it in the right spots */
/** Will probably be MUCH faster if we put somewhere other than Arrow. */
/** We can also batch the inputs and process asynchronously based on memory limitations */
/** There're many variations we can do here */
final class ArrowUnsafeSummer(summer: ArrowSummer, vectorSchemaRoot: VectorSchemaRoot)
  extends UnsafeBatchProcessor {
  private val theVector = vectorSchemaRoot.getFieldVectors.get(0).asInstanceOf[Float8Vector]
  private var counter: Int = 0
  override def insertRow(unsafeRow: UnsafeRow): Unit = {
    val vl = unsafeRow.getDouble(0)

    /**
     * This is bound to be quite slow, allocating memory for each record;
     * TODO find a better strategy
     */
    theVector.setValueCount(counter + 1)
    theVector.setSafe(counter, vl)
    counter = counter + 1
  }
  override def execute(): Iterator[InternalRow] = {
    val result = summer.sum(theVector, 1)
    val row = new UnsafeRow(1)
    val holder = new BufferHolder(row)
    val writer = new UnsafeRowWriter(holder, 2)
    holder.reset()
    writer.write(0, result)
    Iterator(row)
  }
}
