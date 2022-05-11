package com.nec.vectorengine

import com.nec.colvector._
import com.nec.spark.agile.core.CVector
import com.nec.util.CallContext
import scala.concurrent.duration._
import scala.reflect.ClassTag

/*
  NOTE: This is s work in progress
*/
trait VectorEngineMetrics {
  def measureTime[T](collect: FiniteDuration => Unit)(thunk: => T): (T, FiniteDuration) = {
    val start = System.nanoTime
    val result = thunk
    val duration = (System.nanoTime - start).nanoseconds

    collect(duration)
    (result, duration)
  }
}

trait VectorEngine {
  /** The VE process handle that the VectorEngine has access to */
  def process: VeProcess

  /** Return a single dataset - e.g. for maps and filters */
  def execute(lib: LibraryReference,
              fnName: String,
              inputs: Seq[VeColVector],
              outputs: Seq[CVector])
             (implicit context: CallContext): Seq[VeColVector]

  /** Return multiple datasets - e.g. for sorting/exchanges */
  def executeMulti(lib: LibraryReference,
                   fnName: String,
                   inputs: Seq[VeColVector],
                   outputs: Seq[CVector])
                  (implicit context: CallContext): Seq[(Int, Seq[VeColVector])]

  /** Takes in multiple datasets - e.g. for merges */
  def executeMultiIn(lib: LibraryReference,
                     fnName: String,
                     inputs: VeBatchOfBatches,
                     outputs: Seq[CVector])
                    (implicit context: CallContext): Seq[VeColVector]

  /** Takes in multiple batches and returns multiple batches */
  def executeJoin(lib: LibraryReference,
                  fnName: String,
                  left: VeBatchOfBatches,
                  right: VeBatchOfBatches,
                  outputs: Seq[CVector])
                 (implicit context: CallContext): Seq[VeColVector]

  def executeGrouping[K: ClassTag](lib: LibraryReference,
                                   fnName: String,
                                   inputs: VeBatchOfBatches,
                                   outputs: Seq[CVector])
                                  (implicit context: CallContext): Seq[(K, Seq[VeColVector])]
}
