package com.nec.arrow

import org.apache.arrow.vector.{BitVectorHelper, FieldVector, IntVector}

object RichVectors {

  implicit class RichIntVector(vec: FieldVector) {
    def valueSeq[A]: Seq[A] = {
      (0 until vec.getValueCount).map(vec.getObject(_).asInstanceOf[A])
    }

    def validitySeq: Seq[Int] = {
      (0 until vec.getValueCount).map(idx => BitVectorHelper.get(vec.getValidityBuffer, idx))
    }

    def offsetSeq: Seq[Int] = {
      val offsetBuf = vec.getOffsetBuffer
      (0 until vec.getValueCount).map(idx => offsetBuf.getInt(idx))
    }
  }

}
