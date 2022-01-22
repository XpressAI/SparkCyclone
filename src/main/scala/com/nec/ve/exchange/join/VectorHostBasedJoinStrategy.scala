package com.nec.ve.exchange.join

import com.nec.ve.colvector.SharedVectorEngineMemory
import com.nec.ve.colvector.VeColBatch.VeColVector
import org.apache.spark.rdd.RDD

private case object VectorHostBasedJoinStrategy extends VeJoinStrategy with Serializable {
  override def join(
    left: RDD[(Int, List[VeColVector])],
    right: RDD[(Int, List[VeColVector])],
    cleanUpInput: Boolean
  ): RDD[(List[VeColVector], List[VeColVector])] =
    left
      .mapPartitions { iteratorOfRightColumns =>
        import com.nec.ve.VeProcess.OriginalCallingContext.Automatic._

        import com.nec.spark.SparkCycloneExecutorPlugin._
        iteratorOfRightColumns.map { case (idx, batch) =>
          idx -> batch.map(veColVector =>
            try SharedVectorEngineMemory.SharedColVector.fromVeColVector(veColVector)
            finally if (cleanUpInput) veColVector.free()
            else ()
          )
        }
      }
      .join(right.mapPartitions { iteratorOnLeft =>
        import com.nec.ve.VeProcess.OriginalCallingContext.Automatic._

        import com.nec.spark.SparkCycloneExecutorPlugin._
        iteratorOnLeft.map { case (idx, batch) =>
          idx -> batch.map(veColVector =>
            try SharedVectorEngineMemory.SharedColVector.fromVeColVector(veColVector)
            finally if (cleanUpInput) veColVector.free()
            else ()
          )
        }
      })
      .map { case (idx, (leftSharedCols, rightSharedCols)) =>
        import com.nec.spark.SparkCycloneExecutorPlugin._
        import com.nec.ve.VeProcess.OriginalCallingContext.Automatic._
        (leftSharedCols.map(_.toVeColVector()), rightSharedCols.map(_.toVeColVector()))
      }
}
