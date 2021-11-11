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
package org.apache.spark.sql.execution.arrow

import org.apache.arrow.vector._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.execution.arrow.ColumnarArrowWriter.SpecializedColumnVectorGetters
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

object ColumnarArrowWriter {

  final class SpecializedColumnVectorGetters(columnVector: ColumnVector)
    extends SpecializedGetters {
    override def isNullAt(ordinal: Int): Boolean = columnVector.isNullAt(ordinal)
    override def getBoolean(ordinal: Int): Boolean = columnVector.getBoolean(ordinal)
    override def getByte(ordinal: Int): Byte = columnVector.getByte(ordinal)
    override def getShort(ordinal: Int): Short = columnVector.getShort(ordinal)
    override def getInt(ordinal: Int): Int = columnVector.getInt(ordinal)
    override def getLong(ordinal: Int): Long = columnVector.getLong(ordinal)
    override def getFloat(ordinal: Int): Float = columnVector.getFloat(ordinal)
    override def getDouble(ordinal: Int): Double = columnVector.getDouble(ordinal)
    override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal =
      columnVector.getDecimal(ordinal, precision, scale)
    override def getUTF8String(ordinal: Int): UTF8String = columnVector.getUTF8String(ordinal)
    override def getBinary(ordinal: Int): Array[Byte] = columnVector.getBinary(ordinal)
    override def getInterval(ordinal: Int): CalendarInterval = columnVector.getInterval(ordinal)
    override def getStruct(ordinal: Int, numFields: Int): InternalRow =
      columnVector.getStruct(ordinal)
    override def getArray(ordinal: Int): ArrayData = columnVector.getArray(ordinal)
    override def getMap(ordinal: Int): MapData = columnVector.getMap(ordinal)
    override def get(ordinal: Int, dataType: DataType): AnyRef = throw new NotImplementedError(
      ".get is not supported for a general type"
    )
  }

  def arrowSchemaFor(schema: StructType, timeZoneId: String): Schema =
    ArrowUtils.toArrowSchema(schema, timeZoneId)

  def create(schema: StructType, timeZoneId: String): ColumnarArrowWriter = {
    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
    val root = VectorSchemaRoot.create(arrowSchema, ArrowUtils.rootAllocator)
    create(root)
  }

  def create(root: VectorSchemaRoot): ColumnarArrowWriter = {
    val children = root.getFieldVectors().asScala.map { vector =>
      vector.allocateNew()
      createFieldWriter(vector)
    }
    new ColumnarArrowWriter(root, children.toArray)
  }

  private def createFieldWriter(vector: ValueVector): ArrowFieldWriter = {
    val field = vector.getField()
    (ArrowUtils.fromArrowField(field), vector) match {
      case (BooleanType, vector: BitVector)    => new BooleanWriter(vector)
      case (ByteType, vector: TinyIntVector)   => new ByteWriter(vector)
      case (ShortType, vector: SmallIntVector) => new ShortWriter(vector)
      case (IntegerType, vector: IntVector)    => new IntegerWriter(vector)
      case (LongType, vector: BigIntVector)    => new LongWriter(vector)
      case (FloatType, vector: Float4Vector)   => new FloatWriter(vector)
      case (DoubleType, vector: Float8Vector)  => new DoubleWriter(vector)
      case (DecimalType.Fixed(precision, scale), vector: DecimalVector) =>
        new DecimalWriter(vector, precision, scale)
      case (StringType, vector: VarCharVector)             => new StringWriter(vector)
      case (BinaryType, vector: VarBinaryVector)           => new BinaryWriter(vector)
      case (DateType, vector: DateDayVector)               => new DateWriter(vector)
      case (TimestampType, vector: TimeStampMicroTZVector) => new TimestampWriter(vector)
      case (ArrayType(_, _), vector: ListVector) =>
        val elementVector = createFieldWriter(vector.getDataVector())
        new ArrayWriter(vector, elementVector)
      case (MapType(_, _, _), vector: MapVector) =>
        val structVector = vector.getDataVector.asInstanceOf[StructVector]
        val keyWriter = createFieldWriter(structVector.getChild(MapVector.KEY_NAME))
        val valueWriter = createFieldWriter(structVector.getChild(MapVector.VALUE_NAME))
        new MapWriter(vector, structVector, keyWriter, valueWriter)
      case (StructType(_), vector: StructVector) =>
        val children = (0 until vector.size()).map { ordinal =>
          createFieldWriter(vector.getChildByOrdinal(ordinal))
        }
        new StructWriter(vector, children.toArray)
      case (dt, _) =>
        throw new UnsupportedOperationException(s"Unsupported data type: ${dt.catalogString}")
    }
  }
}
class ColumnarArrowWriter(val root: VectorSchemaRoot, fields: Array[ArrowFieldWriter]) {

  def schema: StructType = StructType(fields.map { f =>
    StructField(f.name, f.dataType, f.nullable)
  })

  def writeColumns(columnarBatch: ColumnarBatch): Unit = {
    root.setRowCount(columnarBatch.numRows())
    var colNum = 0
    var rowNum = 0
    val numRows = columnarBatch.numRows()
    while (colNum < columnarBatch.numCols()) {
      val col = columnarBatch.column(colNum)
      val getter = new SpecializedColumnVectorGetters(col)
      while (rowNum < numRows) {
        fields(colNum).write(getter, rowNum)
        rowNum = rowNum + 1
      }
      colNum = colNum + 1
    }
  }
}
