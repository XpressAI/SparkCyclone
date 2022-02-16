package com.nec.ve

import com.nec.ve.serializer.DualBatchOrBytes.{BytesOnly, ColBatchWrapper}

package object serializer {
  val CbTag: Int = 91
  val IntTag: Int = 92
  val MixedCbTagColBatch: Int = 93

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
