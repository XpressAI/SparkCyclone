package org.apache.spark.sql.vectorized

import com.nec.cache.{CachedVeBatch, VeColColumnarVector}
import com.nec.cache.VeColColumnarVector.CachedColumnVector
import com.nec.spark.planning.CEvaluationPlan.HasFloat8Vector.RichObject
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.util.ArrowUtilsExposed

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

  /**
   * Enable the pass-through of [[CachedColumnVector]]s through [[InternalRow]].
   *
   * In case it is not a [[CachedColumnVector]], then we just pass back an iterator of actual [[InternalRow]]
   * that is not cache-based.
   *
   * This is done as a workaround; see more details on the data that is produced in [[com.nec.cache.CycloneCacheBase]]
   */
  def handleIterator(
    iterator: Iterator[InternalRow]
  ): Either[Iterator[List[CachedColumnVector]], Iterator[InternalRow]] = {
    if (!iterator.hasNext) Right(Iterator.empty)
    else {
      iterator.next() match {
        case cbr if cbr.toString.contains(classOf[ColumnarBatchRow].getSimpleName) =>
          Left {
            (Iterator(cbr) ++ iterator)
              .map {
                case cbr if cbr.toString.contains(classOf[ColumnarBatchRow].getSimpleName) =>
                  cbr
                case other =>
                  sys.error(s"Not expected anything other than ColumnarBatchRow, got ${other}")
              }
              .distinct
              .map { cbr =>
                val colVectors: Array[VeColColumnarVector] = cbr.readPrivate.columns.obj
                  .asInstanceOf[Array[ColumnVector]]
                  .map(_.asInstanceOf[VeColColumnarVector])
                colVectors.toList.map(_.dualVeBatch)
              }
          }
        case other =>
          Right(Iterator(other) ++ iterator)
      }
    }
  }

  def cachedBatchesToDualModeInternalRows(
    cachedBatchesIter: Iterator[CachedBatch]
  ): Iterator[InternalRow] = {
    Iterator
      .continually {
        import scala.collection.JavaConverters._
        cachedBatchesIter
          .map(_.asInstanceOf[CachedVeBatch].dualVeBatch.toEither)
          .flatMap {
            case Left(veColBatch) =>
              veColBatch.toInternalColumnarBatch().rowIterator().asScala
            case Right(byteArrayColBatch) =>
              byteArrayColBatch.toInternalColumnarBatch().rowIterator().asScala
          }
      }
      .take(1)
      .flatten
  }

}
