package io.sparkcyclone.data

import scala.util.Try
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.ArrowUtilsExposed

/** This is done as [[SqlConf]] cannot be serialized. */
final case class ColumnBatchEncoding(timezone: String,
                                     targetNumRows: Int,
                                     targetSizeBytes: Long) {
  def makeArrowSchema(schema: Seq[Attribute]): Schema = {
    ArrowUtilsExposed.toArrowSchema(
      schema = StructType(
        schema.map { att =>
          StructField(
            name = att.name,
            dataType = att.dataType,
            nullable = att.nullable,
            metadata = att.metadata
          )
        }
      ),
      timeZoneId = timezone
    )
  }
}

object ColumnBatchEncoding {
  def fromConf(conf: SQLConf)(implicit context: SparkContext): ColumnBatchEncoding = {
    ColumnBatchEncoding(
      Try { conf.sessionLocalTimeZone }.getOrElse("UTC"),
      Try { context.getConf.get("spark.cyclone.ve.columnBatchSize").toInt }.getOrElse(conf.columnBatchSize),
      Try { context.getConf.get("spark.cyclone.ve.targetBatchSizeMb").toLong * Mb }.getOrElse(DefaultTargetBatchSize)
    )
  }

  private val Mb: Long = 1024 * 1024

  // 64M -- trying 128M does not change performance [2022-02-11]
  private val DefaultTargetBatchSize: Long = 64 * Mb
}
