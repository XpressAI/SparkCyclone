package org.apache.spark.sql.util

import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.types.StructType
import org.apache.arrow.vector.types.pojo.Schema

object ArrowUtilsExposed {
  def rootAllocator: RootAllocator = ArrowUtils.rootAllocator
  def toArrowSchema(schema: StructType, timeZoneId: String): Schema =
    ArrowUtils.toArrowSchema(schema, timeZoneId)
}
