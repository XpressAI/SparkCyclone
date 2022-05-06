package com.nec.vectorengine

import com.nec.colvector._
import com.nec.spark.agile.core.{CScalarVector, CVarChar, CVector, VeString}
import com.nec.util.CallContext
import scala.reflect.ClassTag
import com.typesafe.scalalogging.LazyLogging
import org.bytedeco.javacpp.{BytePointer, IntPointer, LongPointer, PointerScope}

trait VectorEngine {
  def execute(lib: LibraryReference,
              fnName: String,
              inputs: Seq[VeColVector],
              outputs: Seq[CVector])(implicit context: CallContext): Seq[VeColVector]
}

class VectorEngineImpl(process: VeProcess,
                       metrics: VectorEngineMetrics)
                       extends VectorEngine with LazyLogging {
  require(process.isOpen, s"VE process is closed: ${process.source}")

  private implicit val p = process

  private[vectorengine] var calls = 0
  private[vectorengine] var veSeconds = 0.0

  def validateVectors(inputs: Seq[VeColVector]): Unit = {
    inputs.foreach { vector =>
      require(
        vector.source == process.source,
        s"Expecting source to be ${process.source}, but got ${vector.source} for vector ${vector}"
      )
    }
  }

  private def readVectorsAsync(pointers: Seq[(Long, CVector)])
                              (implicit context: CallContext): Seq[VeAsyncResult[VeColVector]] = {
    pointers.map { case (location, cvec) =>
      require(location > 0, s"Expected container location to be > 0, got ${location}")
      val buffer = new BytePointer(cvec.veType.containerSize)

      VeAsyncResult(process.getAsync(buffer, location)) { () =>
        val vector = cvec match {
          case CVarChar(name) =>
            VeColVector(
              process.source,
              name,
              VeString,
              buffer.getInt(36),
              Seq(
                buffer.getLong(0),
                buffer.getLong(8),
                buffer.getLong(16),
                buffer.getLong(24)
              ),
              Some(buffer.getInt(32)),
              location
            )

          case CScalarVector(name, stype) =>
            VeColVector(
              process.source,
              name,
              stype,
              buffer.getInt(16),
              Seq(
                buffer.getLong(0),
                buffer.getLong(8)
              ),
              None,
              location
            )
        }

        buffer.close()
        vector
      }
    }
  }

  private def readVectors(pointers: Seq[(Long, CVector)])
                         (implicit context: CallContext): Seq[VeColVector] = {
    readVectorsAsync(pointers).map(_.get)
  }

  final def execFn(lib: LibraryReference,
                   fnName: String,
                   args: Seq[CallStackArgument]): Unit = {
    // Load function symbol
    val func = process.getSymbol(lib, fnName)

    // Load call args stack
    val argstack = process.newArgsStack(args)

    // Execute the function and measure the duration
    logger.debug(s"Calling ${fnName}")
    val (outp, duration) = metrics.measureTime(_ => ()) {
      process.call(func, argstack)
    }

    // Free the call args stack
    process.freeArgsStack(argstack)

    // All Cyclone C++ functions should return 0L on successful completion
    require(outp.get == 0L, s"Expected 0 from function execution, got ${outp.get} instead.")
    outp.close

    // Update counters
    veSeconds += duration.toNanos / 1e9
    calls += 1

    logger.debug(
      s"Finished call to '${func.name}': ${calls} VeSeconds: (${veSeconds} s)"
    )
  }

  def execute(lib: LibraryReference,
              fnName: String,
              inputs: Seq[VeColVector],
              outputs: Seq[CVector])(implicit context: CallContext): Seq[VeColVector] = {
    // Validate columns
    validateVectors(inputs)

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

  /** Return multiple datasets - e.g. for sorting/exchanges */
  def executeMulti(lib: LibraryReference,
                   fnName: String,
                   inputs: Seq[VeColVector],
                   outputs: List[CVector])
                  (implicit context: CallContext): Seq[(Int, Seq[VeColVector])] = {
    val MaxSetsCount = 64

    // Validate columns
    validateVectors(inputs)

    // Set up the input buffer args
    val inbuffers = inputs.map { vec =>
      BuffArg(VeArgIntent.In, new LongPointer(1).put(vec.container))
    }

    // Set up the output count arg
    val countsp = new IntPointer(1L).put(-919)

    // Set up the output buffer args
    val outptrs = outputs.map { _ => new LongPointer(MaxSetsCount).put(-99) }

    // Execute the function
    execFn(lib, fnName, inbuffers ++ Seq(BuffArg(VeArgIntent.Out, countsp)) ++ outptrs.map(BuffArg(VeArgIntent.Out, _)))

    // Ensure that counts is properly set by the function
    val counts = countsp.get
    require(
      counts >= 0 && counts <= MaxSetsCount,
      s"Expected 0 to ${MaxSetsCount} counts; got ${counts} instead. Input args are ${inputs}, outputs are ${outputs}"
    )

    (0 until counts)
      // Read the Nth pointer of each outptrs, and fetch them all to VeColVectors
      .map { set => readVectorsAsync(outptrs.map(_.get(set)).zip(outputs)) }
      // Await each batch fetch
      .map(_.map(_.get))
      // Return with batch indices
      .zipWithIndex.map(_.swap).toSeq
  }

  /** Takes in multiple datasets */
  def executeMultiIn(lib: LibraryReference,
                     fnName: String,
                     inputs: VeBatchOfBatches,
                     outputs: List[CVector])
                    (implicit context: CallContext): Seq[VeColVector] = {
    // Validate columns
    inputs.batches.foreach(batch => validateVectors(batch.columns))

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

  def executeJoin(lib: LibraryReference,
                  fnName: String,
                  left: VeBatchOfBatches,
                  right: VeBatchOfBatches,
                  outputs: Seq[CVector])
                 (implicit context: CallContext): Seq[VeColVector] = {
    // Validate columns
    left.batches.foreach(batch => validateVectors(batch.columns))
    right.batches.foreach(batch => validateVectors(batch.columns))

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

  // def executeGrouping[K: ClassTag](lib: LibraryReference,
  //                                  fnName: String,
  //                                  inputs: VeBatchOfBatches,
  //                                  outputs: Seq[CVector])
  //                                 (implicit context: CallContext): Seq[(K, Seq[VeColVector])] = {
  //   // Validate columns
  //   inputs.batches.foreach(batch => validateVectors(batch.columns))

  //   // Set up the input count args
  //   val countargs = Seq(
  //     // Total batches count for input pointers
  //     U64Arg(inputs.batches.size)
  //   )

  //   // Set up the groups output arg
  //   val groupsp = new LongPointer(1L).put(-919)

  //   // Set up the input buffer args
  //   val inbuffers = inputs.groupedColumns.map { group =>
  //     // For each group, create an array of pointers
  //     val array = group.columns.zipWithIndex
  //       .foldLeft(new LongPointer(group.columns.size.toLong)) { case (accum, (col, i)) =>
  //         accum.put(i.toLong, col.container)
  //       }
  //     BuffArg(VeArgIntent.In, array)
  //   }

  //   // Set up the output buffer args
  //   val outptrs = outputs.map { _ => new LongPointer(1).put(-118) }

  //   // Execute the function
  //   execFn(lib, fnName, countargs ++ Seq(BuffArg(VeArgIntent.Out, groupsp)) ++ inbuffers ++ outptrs.map(BuffArg(VeArgIntent.Out, _)))

  //   // Ensure that groups output pointer is properly set by the function
  //   require(groupsp.get >= 0, s"Groups address is invalid: ${groupsp.get}")

  //   // Fetch the groups
  //   val groups = VeColBatch(readVectors(Seq((groupsp.get, outputs.head))))

  //   val ngroups = groups.numRows
  //   // FIX
  //   val groupKeys = groups.toArray(0)(implicitly[ClassTag[K]], this)

  //   // Declare pointer scope
  //   val scope = new PointerScope()

  //   // Each of the output buffers is an array of pointers to nullable_t_vector's
  //   // so we dereference them first
  //   val actualOutPointers = outptrs.map(_.get)
  //     .map { source =>
  //       val buffer = new LongPointer(ngroups.toLong)
  //       (buffer, process.getAsync(buffer, source))
  //     }
  //     .map { case (buffer, handle) =>
  //       process.awaitResult(handle)
  //       buffer
  //     }

  //   val results = (0 until ngroups)
  //     // Read the Nth pointer of each actualOutPointers, and fetch them all to VeColVectors
  //     .map { set => readVectorsAsync(actualOutPointers.map(_.get(set)).zip(outputs)) }
  //     // Await each batch fetch
  //     .map(_.map(_.get))
  //     // Return with batch indices
  //     .zip(groupKeys).map(_.swap).toSeq

  //   // Free the referenced nullable_t_vector's created from the function call
  //   outptrs.foreach(p => process.unsafeFree(p.get))

  //   // Close scope and return results
  //   scope.close
  //   results
  // }
}

import scala.concurrent.duration._

trait VectorEngineMetrics {
  def measureTime[T](collect: FiniteDuration => Unit)(thunk: => T): (T, FiniteDuration) = {
    val start = System.nanoTime
    val result = thunk
    val duration = (System.nanoTime - start).nanoseconds

    collect(duration)
    (result, duration)
  }
}
