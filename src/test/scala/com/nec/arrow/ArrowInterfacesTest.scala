package com.nec.arrow
import com.nec.arrow.ArrowInterfaces.non_null_double_vector_to_float8Vector
import com.nec.arrow.ArrowTransferStructures.non_null_double_vector
import com.nec.cmake.functions.ParseCSVSpec.RichFloat8
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

import java.nio.ByteBuffer
import java.nio.ByteOrder

final class ArrowInterfacesTest extends AnyFreeSpec {

  List[List[Double]](
    List(5),
    List(5, -5),
    List(5, -5, 10),
    List(5, -5, 10, 11),
    List(5, -5, 10, 11, -91),
    List(5, -5, 10, 11, -91, 5, -5, 10, 11, -91)
  ).foreach { list =>
    s"We can get a vector of size ${list.size} out without facing issues" in {
      val alloc = new RootAllocator(Integer.MAX_VALUE)
      try {
        val vector = new Float8Vector("value", alloc)
        try {
          val ndv = new non_null_double_vector
          ndv.count = list.size
          val bb = ByteBuffer
            .allocateDirect(list.size * 8)
            .order(ByteOrder.LITTLE_ENDIAN)

          list.foreach(bb.putDouble)

          ndv.data = bb
            .asInstanceOf[sun.nio.ch.DirectBuffer]
            .address()
          non_null_double_vector_to_float8Vector(ndv, vector)
          assert(vector.toList == list)
        } finally vector.close()
      } finally alloc.close()
    }
  }

}
