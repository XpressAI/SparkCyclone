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
package org.apache.spark.sql.util
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.{IntVector, SmallIntVector}
import org.apache.spark.sql.types.StructType
object ArrowUtilsExposed {
  def rootAllocator: RootAllocator = ArrowUtils.rootAllocator
  def toArrowSchema(schema: StructType, timeZoneId: String): Schema =
    ArrowUtils.toArrowSchema(schema, timeZoneId)

  implicit class RichSmallIntVector(smallInt: SmallIntVector) {
    def toIntVector: IntVector = {
      val intVector = new IntVector(smallInt.getName, rootAllocator)
      intVector.setValueCount(smallInt.getValueCount)
      (0 until smallInt.getValueCount).foreach {
        case idx if (!smallInt.isNull(idx)) => intVector.set(idx, smallInt.get(idx).toInt)
        case idx                            => intVector.setNull(idx)
      }

      intVector
    }
  }
}
