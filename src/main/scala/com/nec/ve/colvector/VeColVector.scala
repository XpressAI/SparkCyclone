package com.nec.ve.colvector

import com.nec.arrow.colvector.ArrowVectorConversions._
import com.nec.arrow.colvector.{BytePointerColVector, GenericColVector, UnitColVector}
import com.nec.cache.VeColColumnarVector
import com.nec.spark.agile.core.{VeScalarType, VeString, VeType}
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.nec.ve.{VeProcess, VeProcessMetrics}
import org.apache.arrow.vector._
import org.apache.spark.sql.vectorized.ColumnVector
import org.bytedeco.javacpp.BytePointer
import org.slf4j.LoggerFactory
import java.io.OutputStream

// TODO: remove
trait ColVectorUtilsTrait {
  def numItems: Int
  def veType: VeType
  def dataSize: Option[Int]

  private[colvector] def bufferSizes: Seq[Int] = {
    val validitySize = Math.ceil(numItems / 64.0).toInt * 8

    veType match {
      case stype: VeScalarType =>
        Seq(
          numItems * stype.cSize,
          validitySize
        )

      case VeString =>
        dataSize.toSeq.map(_ * 4) ++
        Seq(
          numItems * 4,
          numItems * 4,
          validitySize
        )
    }
  }
}

final case class VeColVector2 private[colvector] (
  source: VeColVectorSource,
  name: String,
  veType: VeType,
  numItems: Int,
  buffers: Seq[Long],
  dataSize: Option[Int],
  container: Long
) extends ColVectorUtilsTrait {
  private val logger = LoggerFactory.getLogger(getClass)
  private var memoryFreed = false

  def allocations: Seq[Long] = {
    Seq(container) ++ buffers
  }

  // TODO: remove toByteArrayColVector
  def serialize(implicit process: VeProcess,
                metrics: VeProcessMetrics): Array[Byte] = {
    val array = metrics.measureRunningTime {
      toBytePointerVector.toByteArrayColVector.serialize
    }(metrics.registerSerializationTime)

    assert(
      array.length == bufferSizes.sum,
      "Resulting array should be same size as sum of all buffer sizes"
    )

    array
  }

  def toStream(stream: OutputStream)(implicit process: VeProcess): Unit = {
    buffers.zip(bufferSizes).foreach { case (start, size) =>
      process.writeToStream(stream, start, size)
    }
  }

  // def toSparkColumnVector: ColumnVector = {
  //   new VeColColumnarVector(Left(this), veType.toSparkType)
  // }

  def toBytePointerVector(implicit process: VeProcess): BytePointerColVector = {
    val nbuffers = buffers.zip(bufferSizes).map { case (location, size) =>
      val ptr = new BytePointer(size)
      process.get(location, ptr, size)
      ptr
    }

    BytePointerColVector(
      source,
      name,
      veType,
      numItems,
      nbuffers
    )
  }

  def toUnitColVector: UnitColVector = {
    UnitColVector(
      source,
      name,
      veType,
      numItems,
      dataSize
    )
  }

  def free(implicit dsource: VeColVectorSource,
           process: VeProcess,
           context: OriginalCallingContext): Unit = {
    if (memoryFreed) {
      logger.warn(s"[VE MEMORY ${container}] double free called!")

    } else {
      require(dsource == source, s"Intended to `free` in ${source}, but got ${dsource} context.")
      allocations.foreach(process.free)
      memoryFreed = true
    }
  }
}

final case class VeColVector(underlying: GenericColVector[Long]) {
  def serializedSize: Int = underlying.bufferSizes.sum

  def serializeToStream(outStream: OutputStream)(implicit veProcess: VeProcess): Unit =
    underlying.buffers.zip(underlying.bufferSizes).foreach { case (bufPos, bufLen) =>
      veProcess.writeToStream(outStream, bufPos, bufLen)
    }

  def allAllocations = containerLocation :: bufferLocations
  def bufferLocations = underlying.buffers
  def containerLocation = underlying.containerLocation
  def source = underlying.source
  def numItems = underlying.numItems
  def name = underlying.name
  def variableSize = underlying.variableSize
  def veType = underlying.veType
  def buffers = underlying.buffers

  import underlying._
  def toInternalVector(): ColumnVector =
    new VeColColumnarVector(Left(this), veType.toSparkType)

  def nonEmpty: Boolean = numItems > 0
  def isEmpty: Boolean = !nonEmpty

  def toUnit: UnitColVector = {
    UnitColVector(
      source,
      name,
      veType,
      numItems,
      variableSize
    )
  }


  /**
   * Retrieve data from veProcess, put it into a Byte Array. Uses bufferSizes.
   */
  def serialize()(implicit veProcess: VeProcess, cycloneMetrics: VeProcessMetrics): Array[Byte] = {
    val totalSize = bufferSizes.sum

    val resultingArray = cycloneMetrics.measureRunningTime(
      toBytePointerVector.toByteArrayColVector.serialize
    )(cycloneMetrics.registerSerializationTime)

    assert(
      resultingArray.length == totalSize,
      "Resulting array should be same size as sum of all buffer sizes"
    )

    resultingArray
  }

  def toBytePointerVector(implicit process: VeProcess): BytePointerColVector = {
    val nbuffers = buffers.zip(bufferSizes).map { case (location, size) =>
      val ptr = new BytePointer(size)
      process.get(location, ptr, size)
      ptr
    }

    BytePointerColVector(
      underlying.source,
      underlying.name,
      underlying.veType,
      underlying.numItems,
      nbuffers
    )
  }

  def free()(implicit dsource: VeColVectorSource,
           process: VeProcess,
           context: OriginalCallingContext): Unit = {
    require(
      dsource == underlying.source,
      s"Intended to `free` in ${underlying.source}, but got ${dsource} context."
    )

    allAllocations.foreach(process.free)
  }
}

//noinspection ScalaUnusedSymbol
object VeColVector {
  def apply(
    source: VeColVectorSource,
    numItems: Int,
    name: String,
    variableSize: Option[Int],
    veType: VeType,
    containerLocation: Long,
    bufferLocations: List[Long]
  ): VeColVector = VeColBatch.VeColVector(
    GenericColVector[Long](
      source = source,
      numItems = numItems,
      name = name,
      variableSize = variableSize,
      veType = veType,
      container = containerLocation,
      buffers = bufferLocations
    )
  )

  def buildContainer(veType: VeType,
                     count: Int,
                     buffers: Seq[Long],
                     dataSizeO: Option[Int])
                    (implicit source: VeColVectorSource,
                     process: VeProcess,
                     context: OriginalCallingContext): Long = {
    veType match {
      case stype: VeScalarType =>
        require(buffers.size == 2, s"Exactly 2 VE buffer pointers are required to construct container for ${stype}")

        // Declare the struct in host memory
        // The layout of `nullable_T_vector` is the same for all T = primitive
        val ptr = new BytePointer(stype.containerSize.toLong)

        // Assign the data, validity, and count values
        ptr.putLong(0, buffers(0))
        ptr.putLong(8, buffers(1))
        ptr.putInt(16, count.abs)

        // Copy the struct to VE and return the VE pointer
        process.putPointer(ptr)

      case VeString =>
        require(buffers.size == 4, s"Exactly 4 VE buffer pointers are required to construct container for ${VeString}")
        require(dataSizeO.nonEmpty, s"datasize is required to construct container for ${VeString}")
        val Some(dataSize) = dataSizeO

        // Declare the struct in host memory
        val ptr = new BytePointer(VeString.containerSize.toLong)

        // Assign the data, validity, and count values
        ptr.putLong(0,  buffers(0))
        ptr.putLong(8,  buffers(1))
        ptr.putLong(16, buffers(2))
        ptr.putLong(24, buffers(3))
        ptr.putInt(32,  dataSize.abs)
        ptr.putInt(36,  count.abs)

        // Copy the struct to VE and return the VE pointer
        process.putPointer(ptr)
    }
  }
}
