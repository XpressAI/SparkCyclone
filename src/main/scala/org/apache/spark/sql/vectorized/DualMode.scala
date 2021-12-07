package org.apache.spark.sql.vectorized

import com.nec.spark.planning.CEvaluationPlan.HasFloat8Vector.RichObject
import com.nec.spark.planning.VeColColumnarVector
import com.nec.ve.VeColBatch
import org.apache.spark.sql.catalyst.InternalRow

object DualMode {

  implicit class RichIterator[T](iterator: Iterator[T]) {
    def distinct: Iterator[T] = {
      iterator
        .sliding(2)
        .zipWithIndex
        .collect {
          case (Seq(one), _)                  => Seq(one)
          case (Seq(a, b), 0) if !a.equals(b) => Seq(a, b)
          case (Seq(a, b), 0) if a.equals(b)  => Seq(a)
          case (Seq(a, b), _) if !a.equals(b) => Seq(b)
        }
        .flatten
    }
  }

  def handleIterator(
    iterator: Iterator[InternalRow]
  ): Either[Iterator[VeColBatch], Iterator[InternalRow]] = {
    if (!iterator.hasNext) Right(Iterator.empty)
    else {
      iterator.next() match {
        case cbr: org.apache.spark.sql.vectorized.ColumnarBatchRow =>
          Left {
            (Iterator(cbr) ++ iterator)
              .map {
                case cbr: org.apache.spark.sql.vectorized.ColumnarBatchRow =>
                  cbr
                case other =>
                  sys.error(s"Not expected anything other than ColumnarBatchRow, got ${other}")
              }
              .distinct
              .map { cbr =>
                val colVectors: Array[VeColColumnarVector] = cbr.readPrivate.columns.obj
                  .asInstanceOf[Array[ColumnVector]]
                  .map(_.asInstanceOf[VeColColumnarVector])
                val vcv = colVectors.toList.map(_.veColVector)
                VeColBatch(vcv.head.numItems, vcv)
              }
          }
        case other =>
          Right(Iterator(other) ++ iterator)
      }
    }
  }

}
