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
package com.nec.spark.planning

import com.nec.util.ReflectionOps._
import org.apache.arrow.vector.{FieldVector, Float8Vector}
import org.apache.commons.lang3.reflect.FieldUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector}

object CEvaluationPlan {
  object HasFieldVector {
    def unapply(columnVector: ColumnVector): Option[FieldVector] = {
      PartialFunction.condOpt(columnVector.readPrivate.accessor.vector.obj) {
        case fv: FieldVector => fv
      }
    }

    implicit class RichColumnVector(columnVector: ColumnVector) {
      def getArrowValueVector: FieldVector = columnVector
        .asInstanceOf[ArrowColumnVector]
        .readPrivate
        .accessor
        .vector
        .obj
        .asInstanceOf[FieldVector]
      def getOptionalArrowValueVector: Option[FieldVector] = Option(columnVector).collect {
        case a: ArrowColumnVector =>
          a.readPrivate.accessor.vector.obj
            .asInstanceOf[FieldVector]
      }
    }
  }

  val batchColumnarBatches = "spark.com.nec.spark.batch-batches"

}
