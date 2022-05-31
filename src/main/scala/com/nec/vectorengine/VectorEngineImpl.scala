package com.nec.vectorengine

import com.nec.cache.TransferDescriptor
import com.nec.colvector._
import com.nec.spark.agile.core.{CScalarVector, CVarChar, CVector, VeString}
import com.nec.util.CallContext
import scala.reflect.ClassTag
import scala.util.Try
import java.time.Duration
import com.codahale.metrics._
import com.typesafe.scalalogging.LazyLogging
import org.bytedeco.javacpp.{BytePointer, IntPointer, LongPointer, PointerScope}

class VectorEngineImpl(val process: VeProcess,
                       val metrics: MetricRegistry)
                       extends VectorEngine with LazyLogging {
  require(process.isOpen, s"VE process is closed: ${process.source}")

  // Declare implicit for use with VeAsyncResult contexts
  private implicit val p = process

  implicit class VeColVectorExtensions(vector: VeColVector) {
    def register: VeColVector = {
      // Register the buffers for allocations tracking
      vector.buffers.zip(vector.bufferSizes).foreach { case (address, size) =>
        process.registerAllocation(address, size)
      }
      // Register the nullable_t_struct itself for allocations tracking
      process.registerAllocation(vector.container, vector.veType.containerSize)
      vector
    }

    def validate: Unit = {
      require(
        vector.source == process.source,
        s"Expecting source to be ${process.source}, but got ${vector.source} for vector ${vector}"
      )
    }
  }

  private[vectorengine] def readVectorsAsync(pointers: Seq[(Long, CVector)])
                                            (implicit context: CallContext): Seq[VeAsyncResult[VeColVector]] = {
    pointers.map { case (location, descriptor) =>
      require(location > 0, s"Expected nullable_t_struct container location to be > 0L, got ${location}")
      val buffer = new BytePointer(descriptor.veType.containerSize)

      // Copy the nullable_t_struct to VH memory and wait
      VeAsyncResult(process.getAsync(buffer, location)) { () =>
        // Create the VeColVector from the contents of the nullable_t_struct buffer in VH memory
        val vector = VeColVector.fromBuffer(buffer, location, descriptor)(process.source)
        // Close the buffer
        buffer.close
        // Register the VE-allocated memory for VeProcess tracking and return
        vector.register
      }
    }
  }

  private[vectorengine] def readVectors(pointers: Seq[(Long, CVector)])
                                       (implicit context: CallContext): Seq[VeColVector] = {
    readVectorsAsync(pointers).map(_.get)
  }

  private[vectorengine] final def execFn(lib: LibraryReference,
                                         fnName: String,
                                         args: Seq[CallStackArgument])
                                        (implicit context: CallContext): Unit = {
    // Load function symbol
    val func = process.getSymbol(lib, fnName)

    // Load call args stack
    val argstack = process.newArgsStack(args)

    // Execute the function
    logger.debug(s"Calling VE function: ${fnName}")
    val outp = process.call(func, argstack)

    // Free the call args stack
    process.freeArgsStack(argstack)

    // All Cyclone C++ functions should return 0L on successful completion
    require(outp.get == 0L, s"Expected 0 from function execution, got ${outp.get} instead: ${fnName} (${lib.path})")
    outp.close
  }

  private[vectorengine] def measureTime[T](fnName: String)(thunk: => T): T = {
    val start = System.nanoTime
    // Perform the execution
    val result = thunk
    val duration = System.nanoTime - start

    // Add metric to timer
    val tname = s"${VectorEngine.ExecCallDurationsMetric}.${fnName}"
    val timer = Option(metrics.getTimers.get(tname)) match {
      case Some(timer) =>
        timer

      case None =>
        val timer = new Timer
        Try { metrics.register(tname, timer) }
        timer
    }
    timer.update(Duration.ofNanos(duration))

    // Return result
    result
  }

  def execute(lib: LibraryReference,
              fnName: String,
              inputs: Seq[VeColVector],
              outputs: Seq[CVector])
             (implicit context: CallContext): Seq[VeColVector] = {
    measureTime(fnName) {
      // Validate columns
      inputs.foreach(_.validate)

      // Set up the input buffer args
      val inbuffers = inputs.map { vec =>
        BuffArg(VeArgIntent.In, new LongPointer(1).put(vec.container))
      }

      // Set up the output buffer args
      val outptrs = outputs.map { _ => new LongPointer(1).put(-118) }

      // Execute the function
      execFn(lib, fnName, inbuffers ++ outptrs.map(BuffArg(VeArgIntent.Out, _)))

      // Read the VeColVector structs back
      readVectors(outptrs.map(_.get).zip(outputs))
    }
  }

  def executeMulti(lib: LibraryReference,
                   fnName: String,
                   inputs: Seq[VeColVector],
                   outputs: Seq[CVector])
                  (implicit context: CallContext): Seq[(Int, Seq[VeColVector])] = {
    measureTime(fnName) {
      // Validate columns
      inputs.foreach(_.validate)

      // Set up the input buffer args
      val inbuffers = inputs.map { vec =>
        BuffArg(VeArgIntent.In, new LongPointer(1).put(vec.container))
      }

      // Set up the output count arg
      val countsp = new IntPointer(1L).put(-919)

      // Set up the output buffer args
      val outptrs = outputs.map { _ => new LongPointer(VectorEngine.MaxSetsCount).put(-99) }

      // Execute the function
      execFn(lib, fnName, inbuffers ++ Seq(BuffArg(VeArgIntent.Out, countsp)) ++ outptrs.map(BuffArg(VeArgIntent.Out, _)))

      // Ensure that counts is properly set by the function
      val counts = countsp.get
      require(
        counts >= 0 && counts <= VectorEngine.MaxSetsCount,
        s"Expected 0 to ${VectorEngine.MaxSetsCount} counts; got ${counts} instead. Input args are ${inputs}, outputs are ${outputs}"
      )

      (0 until counts)
        // Read the Nth pointer of each outptrs, and fetch them all to VeColVectors
        .map { set => readVectorsAsync(outptrs.map(_.get(set)).zip(outputs)) }
        // Await each batch fetch
        .map(_.map(_.get))
        // Return with batch indices
        .zipWithIndex.map(_.swap).toSeq
    }
  }

  def executeMultiIn(lib: LibraryReference,
                     fnName: String,
                     inputs: VeBatchOfBatches,
                     outputs: Seq[CVector])
                    (implicit context: CallContext): Seq[VeColVector] = {
    measureTime(fnName) {
      // Validate columns
      inputs.batches.flatMap(_.columns).foreach(_.validate)

      // Set up the input count args
      val countargs = Seq(
        // Total batches count for input pointers
        I32Arg(inputs.batches.size),
        // Output count of rows - better to know this in advance
        I32Arg(inputs.numRows)
      )

      // Set up the input buffer args
      val inbuffers = inputs.groupedColumns.map { group =>
        // For each group, create an array of pointers
        val array = group.columns.zipWithIndex
          .foldLeft(new LongPointer(group.columns.size.toLong)) { case (accum, (col, i)) =>
            accum.put(i.toLong, col.container)
          }
        BuffArg(VeArgIntent.In, array)
      }

      // Set up the output buffer args
      val outptrs = outputs.map { _ => new LongPointer(1).put(-118) }

      // Execute the function
      execFn(lib, fnName, countargs ++ inbuffers ++ outptrs.map(BuffArg(VeArgIntent.Out, _)))

      // Read the VeColVector structs back
      readVectors(outptrs.map(_.get).zip(outputs))
    }
  }

  def executeJoin(lib: LibraryReference,
                  fnName: String,
                  left: VeBatchOfBatches,
                  right: VeBatchOfBatches,
                  outputs: Seq[CVector])
                 (implicit context: CallContext): Seq[VeColVector] = {
    measureTime(fnName) {
      // Validate columns
      left.batches.flatMap(_.columns).foreach(_.validate)
      right.batches.flatMap(_.columns).foreach(_.validate)

      // Set up the input count args
      val countargs = Seq(
        // Total batches count for the left & right input pointers
        U64Arg(left.batches.size),
        U64Arg(right.batches.size),
        // Input count of rows - better to know this in advance
        U64Arg(left.numRows),
        U64Arg(right.numRows)
      )

      // Set up the left buffer args
      val leftbuffers = left.groupedColumns.map { group =>
        // For each group, create an array of pointers
        val array = group.columns.zipWithIndex
          .foldLeft(new LongPointer(group.columns.size.toLong)) { case (accum, (col, i)) =>
            accum.put(i.toLong, col.container)
          }
        BuffArg(VeArgIntent.In, array)
      }

      // Set up the right buffer args
      val rightbuffers = right.groupedColumns.map { group =>
        // For each group, create an array of pointers
        val array = group.columns.zipWithIndex
          .foldLeft(new LongPointer(group.columns.size.toLong)) { case (accum, (col, i)) =>
            accum.put(i.toLong, col.container)
          }
        BuffArg(VeArgIntent.In, array)
      }

      // Set up the output buffer args
      val outptrs = outputs.map { _ => new LongPointer(1).put(-118) }

      // Execute the function
      execFn(lib, fnName, countargs ++ leftbuffers ++ rightbuffers ++ outptrs.map(BuffArg(VeArgIntent.Out, _)))

      // Read the VeColVector structs back
      readVectors(outptrs.map(_.get).zip(outputs))
    }
  }

  def executeGrouping[K: ClassTag](lib: LibraryReference,
                                   fnName: String,
                                   inputs: VeBatchOfBatches,
                                   outputs: Seq[CVector])
                                  (implicit context: CallContext): Seq[(K, Seq[VeColVector])] = {
    measureTime(fnName) {
      // Validate columns
      inputs.batches.flatMap(_.columns).foreach(_.validate)

      // Set up the input count args
      val countargs = Seq(
        // Total batches count for input pointers
        U64Arg(inputs.batches.size)
      )

      // Set up the groups output arg
      val groupsp = new LongPointer(1L).put(-919)

      // Set up the input buffer args
      val inbuffers = inputs.groupedColumns.map { group =>
        // For each group, create an array of pointers
        val array = group.columns.zipWithIndex
          .foldLeft(new LongPointer(group.columns.size.toLong)) { case (accum, (col, i)) =>
            accum.put(i.toLong, col.container)
          }
        BuffArg(VeArgIntent.In, array)
      }

      // Set up the output buffer args
      val outptrs = outputs.map { _ => new LongPointer(1).put(-118) }

      // Execute the function
      execFn(lib, fnName, countargs ++ Seq(BuffArg(VeArgIntent.Out, groupsp)) ++ inbuffers ++ outptrs.map(BuffArg(VeArgIntent.Out, _)))

      // Ensure that groups output pointer is properly set by the function
      require(groupsp.get >= 0, s"Groups address is invalid: ${groupsp.get}")

      // Fetch the groups
      val groups = VeColBatch(readVectors(Seq((groupsp.get, outputs.head))))
      val ngroups = groups.numRows

      // Fetch the group keys
      val groupKeys = groups.toArray2[K](0)

      // Declare pointer scope
      val scope = new PointerScope

      // Each of the output buffers is an array of pointers to nullable_t_vector's
      // so we dereference them first
      val actualOutPointers = outptrs.map(_.get)
        .map { source =>
          val buffer = new LongPointer(ngroups.toLong)
          (buffer, process.getAsync(buffer, source))
        }
        .map { case (buffer, handle) =>
          process.awaitResult(handle)
          buffer
        }

      val results = (0 until ngroups)
        // Read the Nth pointer of each actualOutPointers, and fetch them all to VeColVectors
        .map { set => readVectorsAsync(actualOutPointers.map(_.get(set)).zip(outputs)) }
        // Await each batch fetch
        .map(_.map(_.get))
        // Return with batch indices
        .zip(groupKeys).map(_.swap).toSeq

      // Free the referenced nullable_t_vector's created from the function call
      outptrs.foreach(p => process.free(p.get, unsafe = true))

      // Close scope and return results
      scope.close
      results
    }
  }

  def executeTransfer(lib: LibraryReference,
                      descriptor: TransferDescriptor)
                     (implicit context: CallContext): VeColBatch = {
    require(descriptor.nonEmpty, "TransferDescriptor is empty")

    // Allocate the buffer in VE and transfer the data over
    logger.debug("Allocating VE memory and transferring data over using TransferDescriptor...")
    val allocation = process.put(descriptor.buffer)

    // Unregister from VeProcess tracking, as the memory is going to be freed on the VE during transfer handling
    process.unregisterAllocation(allocation.address)

    // Unpack and construct nullabble_t_struct's from the VE side
    execFn(lib, LibCyclone.HandleTransferFn, Seq(
      BuffArg(VeArgIntent.In, new LongPointer(1).put(allocation.address)),
      BuffArg(VeArgIntent.Out, descriptor.resultBuffer)
    ))

    // Construct VeColBatch from the output buffer
    val batch = descriptor.resultToColBatch

    // Free the transfer buffer and the result buffer on the VH side
    descriptor.close

    // Register the allocations made from the VE
    batch.columns.foreach(_.register)
    batch
  }

  def executeTransfer(descriptor: TransferDescriptor)
                     (implicit context: CallContext): VeColBatch = {
    require(descriptor.nonEmpty, "TransferDescriptor is empty")

    // Load libcyclone if not already loaded
    val lib = process.load(LibCyclone.SoPath)

    executeTransfer(lib, descriptor)
  }
}
