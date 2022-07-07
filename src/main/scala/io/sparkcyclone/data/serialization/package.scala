package io.sparkcyclone.data

import io.sparkcyclone.colvector.VeColBatch
import io.sparkcyclone.data.serialization.DualBatchOrBytes.{BytesOnly, ColBatchWrapper}

package object serialization {
  val CbTag: Int = 91
  val IntTag: Int = 92
  val MixedCbTagColBatch: Int = 93
  val LongTag: Int = 94
  val DoubleTag: Int = 95

  sealed trait DualBatchOrBytes {
    def fold[T](a: BytesOnly => T, b: VeColBatch => T): T = this match {
      case ColBatchWrapper(veColBatch) => b(veColBatch)
      case bytesOnly: BytesOnly        => a(bytesOnly)
    }
  }
  object DualBatchOrBytes {
    final case class ColBatchWrapper(veColBatch: VeColBatch) extends DualBatchOrBytes
    /** Underlying byte buffer may be re-used for performance */
    final class BytesOnly(val bytes: Array[Byte], val size: Int) extends DualBatchOrBytes
  }
}
