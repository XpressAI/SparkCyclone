package com.nec.ve.serializer

import com.nec.colvector.VeColBatch

sealed trait DualBatchOrBytes extends Serializable {
  def fold[T](a: BytesOnly => T, b: VeColBatch => T): T = this match {
    case ColBatchWrapper(veColBatch) => b(veColBatch)
    case bytesOnly: BytesOnly        => a(bytesOnly)
  }
}

/** Underlying byte buffer may be re-used for performance */
final class BytesOnly(val bytes: Array[Byte], val size: Int)
  extends DualBatchOrBytes
    with Serializable

final case class ColBatchWrapper(veColBatch: VeColBatch) extends DualBatchOrBytes with Serializable
