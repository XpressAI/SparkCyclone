package com.nec.cache

import com.nec.cache.VeColColumnarVector.CachedColumnVector
import com.nec.util.ReflectionOps._
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.colvector.VeColBatch.VeColVectorSource
import com.nec.ve.{VeColBatch, VeProcess, VeProcessMetrics}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.vectorized.ColumnVector

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

  /** We have to use the name, because this class is not accessible to us by default */
  val SparkColumnarBatchRowClassName = "ColumnarBatchRow"

  /**
   * Enable the pass-through of [[CachedColumnVector]]s through [[InternalRow]].
   *
   * In case it is not a [[CachedColumnVector]], then we just pass back an iterator of actual [[InternalRow]]
   * that is not cache-based.
   *
   * This is done as a workaround; see more details on the data that is produced in [[com.nec.cache.CycloneCacheBase]]
   */
  def unwrapInternalRows(
    iterator: Iterator[InternalRow]
  ): Either[Iterator[List[CachedColumnVector]], Iterator[InternalRow]] = {
    if (!iterator.hasNext) Right(Iterator.empty)
    else {
      iterator.next() match {
        case cbr if cbr.toString.contains(SparkColumnarBatchRowClassName) =>
          Left {
            (Iterator(cbr) ++ iterator)
              .map {
                case cbr if cbr.toString.contains(SparkColumnarBatchRowClassName) =>
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

  def unwrapPossiblyDualToVeColBatches(
    possiblyDualModeInternalRows: Iterator[InternalRow],
    arrowSchema: Schema,
    metricsFn: (() => VeColBatch) => VeColBatch = (x) => { x() }
  )(implicit
    bufferAllocator: BufferAllocator,
    veProcess: VeProcess,
    source: VeColVectorSource,
    arrowEncodingSettings: ArrowEncodingSettings,
    originalCallingContext: OriginalCallingContext,
    cycloneMetrics: VeProcessMetrics
  ): Iterator[VeColBatch] =
    DualMode.unwrapInternalRows(possiblyDualModeInternalRows) match {
      case Left(colBatches) =>
        colBatches.map(cachedColumnVectors =>
          metricsFn { () =>
            DualColumnarBatchContainer(vecs = cachedColumnVectors).toVEColBatch()
          }
        )
      case Right(rowIterator) =>
        SparkInternalRowsToArrowColumnarBatches
          .apply(rowIterator = rowIterator, arrowSchema = arrowSchema)
          .map { columnarBatch =>
            /* cleaning up the [[columnarBatch]] is not necessary as the underlying ones does it */
            metricsFn { () =>
              VeColBatch.fromArrowColumnarBatch(columnarBatch)
            }
          }
    }

}
