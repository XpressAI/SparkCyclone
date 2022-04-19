package com.nec.colvector

import com.nec.ve.{VeProcess, VeProcessMetrics}
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.colvector.VeColBatch.VeColVectorSource


final case class UnitColBatch(columns: Seq[UnitColVector]) {
  require(
    columns.nonEmpty,
    s"[${getClass.getName}] Number of columns needs to be > 0"
  )

  def withData(arrays: Seq[Array[Byte]])(implicit source: VeColVectorSource,
                                         process: VeProcess,
                                         context: OriginalCallingContext,
                                         metrics: VeProcessMetrics): VeColBatch = {
    val ncolumns = columns.zip(arrays).map { case (colvec, bytes) =>
      colvec.withData(bytes)
    }

    VeColBatch(GenericColBatch(ncolumns.head.numItems, ncolumns.toList))
  }
}

// final case class UnitColBatch(underlying: GenericColBatch[UnitColVector]) {
//   def deserialize(seqs: List[Array[Byte]])(implicit
//     veProcess: VeProcess,
//     originalCallingContext: OriginalCallingContext,
//     veColVectorSource: VeColVectorSource,
//     cycloneMetrics: VeProcessMetrics
//   ): VeColBatch = {
//     val theMap = underlying.cols
//       .zip(seqs)
//       .map { case (unitCv, bytes) =>
//         unitCv -> unitCv.withData(bytes)
//       }
//       .toMap

//     VeColBatch(underlying.map(ucv => theMap(ucv)))
//   }
// }
