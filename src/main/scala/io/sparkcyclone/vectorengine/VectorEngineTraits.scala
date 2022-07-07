package io.sparkcyclone.vectorengine

import com.codahale.metrics.MetricRegistry
import io.sparkcyclone.colvector._
import io.sparkcyclone.data.transfer.TransferDescriptor
import io.sparkcyclone.spark.agile.core.CVector
import io.sparkcyclone.util.CallContext

import scala.reflect.ClassTag

trait VectorEngine {
  /** The VE process handle that the VectorEngine has access to */
  def process: VeProcess

  def metrics: MetricRegistry

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

  def executeTransfer(lib: LibraryReference,
                      descriptor: TransferDescriptor)
                     (implicit context: CallContext): VeColBatch

  def executeTransfer(descriptor: TransferDescriptor)
                     (implicit context: CallContext): VeColBatch
}

object VectorEngine {
  final val ExecCallDurationsMetric = "ve.durations.exec"
  final val MaxSetsCount = 64
}
