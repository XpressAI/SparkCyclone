package com.nec.arrow
import com.nec.arrow.CountArrowStringsSpec.schema
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.VarCharVector

import java.util

object ArrowVectorBuilders {
  def withArrowStringVector[T](stringBatch: Seq[String])(f: VarCharVector => T): T = {
    import org.apache.arrow.vector.VectorSchemaRoot
    WithTestAllocator { alloc =>
      val vcv = schema.findField("value").createVector(alloc).asInstanceOf[VarCharVector]
      vcv.allocateNew()
      try {
        val root = new VectorSchemaRoot(schema, util.Arrays.asList(vcv: FieldVector), 2)
        stringBatch.view.zipWithIndex.foreach { case (str, idx) =>
          vcv.setSafe(idx, str.getBytes("utf8"), 0, str.length)
        }
        vcv.setValueCount(stringBatch.length)
        root.setRowCount(stringBatch.length)
        f(vcv)
      } finally vcv.close()
    }
  }

  def withArrowFloat8Vector[T](inputColumns: Seq[Seq[Double]])(f: Float8Vector => T): T = {
    WithTestAllocator { alloc =>
      val data = inputColumns.flatten
      val vcv = new Float8Vector("value", alloc)
      vcv.allocateNew()
      try {
        inputColumns.flatten.zipWithIndex.foreach { case (str, idx) =>
          vcv.setSafe(idx, str)
        }
        if (data.nonEmpty)
          vcv.setValueCount(data.size)

        f(vcv)
      } finally vcv.close()
    }
  }

  def withDirectFloat8Vector[T](data: Seq[Double])(f: Float8Vector => T): T =
    WithTestAllocator { alloc =>
      val vcv = new Float8Vector("value", alloc)
      vcv.allocateNew()
      try {
        data.zipWithIndex.foreach { case (str, idx) =>
          vcv.setSafe(idx, str)
        }
        vcv.setValueCount(data.size)

        f(vcv)
      } finally vcv.close()
    }

  def withDirectIntVector[T](data: Seq[Int])(f: IntVector => T): T = {
    WithTestAllocator { alloc =>
      val vcv = new IntVector("value", alloc)
      vcv.allocateNew()
      try {
        data.zipWithIndex.foreach { case (str, idx) =>
          vcv.setSafe(idx, str)
        }
        vcv.setValueCount(data.size)

        f(vcv)
      } finally vcv.close()
    }
  }
}
