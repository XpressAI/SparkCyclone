package com.nec

import java.nio.ByteBuffer

package object ve {
  type MaybeByteArrayColVector = ColVector[Option[Array[Byte]]]
  type ByteBufferVeColVector = ColVector[Option[ByteBuffer]]
}
