/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.nec.arrow
import com.nec.arrow.CountArrowStringsSpec.schema
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BigIntVector, FieldVector, Float8Vector, IntVector, VarCharVector}

import java.util

object ArrowVectorBuilders {
  var vectorCount = 0

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

  def withNullableArrowStringVector[T](
    stringBatch: Seq[Option[String]]
  )(f: VarCharVector => T): T = {
    import org.apache.arrow.vector.VectorSchemaRoot
    WithTestAllocator { alloc =>
      val vcv = schema.findField("value").createVector(alloc).asInstanceOf[VarCharVector]
      vcv.allocateNew()
      try {
        val root = new VectorSchemaRoot(schema, util.Arrays.asList(vcv: FieldVector), 2)
        stringBatch.view.zipWithIndex.foreach {
          case (Some(str), idx) => vcv.setSafe(idx, str.getBytes("utf8"), 0, str.length)
          case (None, idx)      => vcv.setNull(idx)
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
      val vcv = new Float8Vector(s"value$vectorCount", alloc)
      vectorCount += 1
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
  def withArrowFloat8VectorI[T](
    data: Seq[Double]
  )(f: Float8Vector => T)(implicit alloc: BufferAllocator): T = {
    val vcv = new Float8Vector(s"value$vectorCount", alloc)
    vectorCount += 1
    vcv.allocateNew()
    try {
      data.zipWithIndex.foreach { case (str, idx) =>
        vcv.setSafe(idx, str)
      }

      if (data.nonEmpty) {
        vcv.setValueCount(data.size)
      }

      f(vcv)
    } finally vcv.close()
  }

  def withDirectFloat8Vector[T](data: Seq[Double])(f: Float8Vector => T): T =
    WithTestAllocator { alloc =>
      val vcv = new Float8Vector(s"value$vectorCount", alloc)
      vectorCount += 1
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
      val vcv = new IntVector(s"value$vectorCount", alloc)
      vectorCount += 1
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

  def withNullableIntVector[T](data: Seq[Option[Int]])(f: IntVector => T): T = {
    WithTestAllocator { alloc =>
      val vcv = new IntVector(s"value$vectorCount", alloc)
      vectorCount += 1
      vcv.allocateNew()
      try {
        data.zipWithIndex.foreach {
          case (None, idx)      => vcv.setNull(idx)
          case (Some(str), idx) => vcv.setSafe(idx, str)
        }
        vcv.setValueCount(data.size)

        f(vcv)
      } finally vcv.close()
    }
  }

  def withDirectBigIntVector[T](data: Seq[Long])(f: BigIntVector => T): T = {
    WithTestAllocator { alloc =>
      val vcv = new BigIntVector(s"value$vectorCount", alloc)
      vectorCount += 1
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
