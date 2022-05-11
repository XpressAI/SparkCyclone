package com.nec.spark.planning.plans

import com.nec.cache.{ArrowEncodingSettings, CycloneCacheBase, DualMode, TransferDescriptor}
import com.nec.colvector.ArrowVectorConversions.ValueVectorToBPCV
import com.nec.colvector.SparkSqlColumnVectorConversions.{SparkSqlColumnVectorToArrow, SparkSqlColumnVectorToBPCV}
import com.nec.colvector.VeColBatch
import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.spark.planning._
import com.nec.ve.VeKernelCompiler
import com.nec.ve.VeProcess.OriginalCallingContext
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.util.ArrowUtilsExposed

import java.nio.file.Paths

// SparkToVectorEnginePlan calls handleTransfer, a library function. It uses the parentVeFunction
// to get access to the library.
case class SparkToVectorEnginePlan(childPlan: SparkPlan, parentVeFunction: VeFunction)
  extends UnaryExecNode
  with LazyLogging
  with SupportsVeColBatch
  with PlanCallsVeFunction
  with PlanMetrics {

  override lazy val metrics = invocationMetrics(PLAN) ++ invocationMetrics(VE) ++ batchMetrics(INPUT) ++ batchMetrics(OUTPUT) ++ invocationMetrics("Conversion") ++ batchMetrics("byte")

  override protected def doCanonicalize(): SparkPlan = super.doCanonicalize()

  override def child: SparkPlan = childPlan

  override def output: Seq[Attribute] = child.output

  override def dataCleanup: DataCleanup = DataCleanup.cleanup(this.getClass)

  private def metricsFn[T](f:() => T): T = withInvocationMetrics(VE)(f.apply())

  override def executeVeColumnar(): RDD[VeColBatch] = {
    require(!child.isInstanceOf[SupportsVeColBatch], "Child should not be a VE plan")
    initializeMetrics()
    val byteTotalBatchRowCount = longMetric(s"byteTotalBatchRowCount")

    // Instead of creating a new config we are reusing columnBatchSize. In the future if we do
    // combine with some of the Arrow conversion tools we will need to unify some of the configs.
    implicit val arrowEncodingSettings = ArrowEncodingSettings.fromConf(conf)(sparkContext)

    if (child.supportsColumnar) {
      child
        .executeColumnar()
        .mapPartitions { columnarBatches =>
          withInvocationMetrics(PLAN) {
            import SparkCycloneExecutorPlugin._
            implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
              .newChildAllocator(s"Writer for partial collector (ColBatch-->Arrow)", 0, Long.MaxValue)
            TaskContext.get().addTaskCompletionListener[Unit](_ => allocator.close())
            import OriginalCallingContext.Automatic._

            // Transfer entire partition, i.e. all batches, with a single transfer.
            // Steps:
            // 1. Collect BytePointerVolVector from all batches
            // 2. Use TransferDescriptor.Builder to build the transfer descriptor with folds
            // 3. Call .build() on builder to create the transfer descriptor
            //    (i.e. create the one buffer to copy)
            // 4. Call veProcess.executeTransfer(transferDescriptor) to transfer the data and
            //    unpack it into a format which is expected by the rest of the application

            val arrowSchema = CycloneCacheBase.makeArrowSchema(child.output)

            val transferDescriptor = withInvocationMetrics("Conversion"){
              collectBatchMetrics(INPUT, columnarBatches).zipWithIndex.map { case (columnarBatch, idx) =>
                (0 until columnarBatch.numCols())
                  .map { i =>
                    columnarBatch.column(i).getOptionalArrowValueVector match {
                      case Some(acv) =>
                        acv.toBytePointerColVector
                      case None =>
                        val field = arrowSchema.getFields.get(i)
                        columnarBatch.column(i)
                          .toBytePointerColVector(field.getName, columnarBatch.numRows)
                    }
                  }
              }.foldLeft(new TransferDescriptor.Builder()){ case (builder, batch) =>
                builder.newBatch().addColumns(batch)
              }.build()
            }

            collectBatchMetrics(OUTPUT, if(transferDescriptor.isEmpty){
              logger.debug("Empty transfer descriptor")
              Iterator.empty
            }else{
              // TODO: find a better way of calling a library function ("handle_transfer") from here
              val libRef = veProcess.loadLibrary(Paths.get(veFunction.libraryPath).getParent.resolve("sources").resolve(VeKernelCompiler.PlatformLibrarySoName))
              val batch = withInvocationMetrics(VE) {
                veProcess.executeTransfer(
                  libraryReference = libRef,
                  transferDescriptor = transferDescriptor
                )
              }

              Seq(batch).iterator
            })
          }
        }
    } else {
      child.execute().mapPartitions { internalRows =>
        import SparkCycloneExecutorPlugin._
        import ImplicitMetrics._

        withInvocationMetrics(PLAN){
          implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
            .newChildAllocator(s"Writer for partial collector (Arrow)", 0, Long.MaxValue)
          TaskContext.get().addTaskCompletionListener[Unit](_ => allocator.close())
          import OriginalCallingContext.Automatic._

          collectBatchMetrics(OUTPUT, DualMode.unwrapPossiblyDualToVeColBatches(
            possiblyDualModeInternalRows = internalRows,
            arrowSchema = CycloneCacheBase.makeArrowSchema(child.output),
            metricsFn
          ))
        }
      }
    }
  }

  override def veFunction: VeFunction = parentVeFunction

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(parentVeFunction = f(parentVeFunction))
}
