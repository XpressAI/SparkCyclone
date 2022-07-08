package io.sparkcyclone.spark.plans

import io.sparkcyclone.cache.{ArrowEncodingSettings, CycloneCacheBase}
import io.sparkcyclone.data.conversion.ArrowVectorConversions.ValueVectorToBPCV
import io.sparkcyclone.data.conversion.SparkSqlColumnVectorConversions.{SparkSqlColumnVectorToArrow, SparkSqlColumnVectorToBPCV}
import io.sparkcyclone.data.vector.VeColBatch
import io.sparkcyclone.data.transfer.{BpcvTransferDescriptor, RowCollectingTransferDescriptor}
import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin._
import io.sparkcyclone.spark.planning._
import io.sparkcyclone.util.CallContextOps._
import io.sparkcyclone.vectorengine.LibCyclone
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, OrderedDistribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{RowToColumnarTransition, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.util.ArrowUtilsExposed

// SparkToVectorEnginePlan calls handleTransfer, a library function. It uses the parentVeFunction
// to get access to the library.
case class SparkToVectorEnginePlan(childPlan: SparkPlan, parentVeFunction: VeFunction, sortOrder: Option[Seq[SortOrder]] = None)
  extends UnaryExecNode
  with LazyLogging
  with SupportsVeColBatch
  with PlanCallsVeFunction
  with RowToColumnarTransition
  with PlanMetrics {

  override lazy val metrics = invocationMetrics(PLAN) ++ invocationMetrics(VE) ++ batchMetrics(INPUT) ++ batchMetrics(OUTPUT) ++ invocationMetrics("Conversion") ++ invocationMetrics("Materialization") ++ batchMetrics("byte")

  override protected def doCanonicalize(): SparkPlan = super.doCanonicalize()

  override def child: SparkPlan = childPlan

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = sortOrder.getOrElse(Nil)

  override def requiredChildDistribution: Seq[Distribution] = if (sortOrder.isDefined) Seq(OrderedDistribution(sortOrder.get)) else Seq(UnspecifiedDistribution)

  override def dataCleanup: DataCleanup = DataCleanup.cleanup(this.getClass)

  private def metricsFn[T](f:() => T): T = withInvocationMetrics(VE)(f.apply())

  override def executeVeColumnar(): RDD[VeColBatch] = {
    require(!child.isInstanceOf[SupportsVeColBatch], "Child should not be a VE plan")
    initializeMetrics()
    val byteTotalBatchRowCount = longMetric(s"byteTotalBatchRowCount")

    // Instead of creating a new config we are reusing columnBatchSize. In the future if we do
    // combine with some of the Arrow conversion tools we will need to unify some of the configs.
    implicit val encoding = ArrowEncodingSettings.fromConf(conf)(sparkContext)

    if (child.supportsColumnar) {
      child
        .executeColumnar()
        .mapPartitions { columnarBatches =>
          withInvocationMetrics(PLAN) {
            implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
              .newChildAllocator(s"Writer for partial collector (ColBatch-->Arrow)", 0, Long.MaxValue)
            TaskContext.get().addTaskCompletionListener[Unit](_ => allocator.close())

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
              }.foldLeft(new BpcvTransferDescriptor.Builder()){ case (builder, batch) =>
                builder.newBatch().addColumns(batch)
              }.build()
            }

            collectBatchMetrics(OUTPUT, if(transferDescriptor.isEmpty){
              logger.debug("Empty transfer descriptor")
              Iterator.empty
            }else{
              // TODO: find a better way of calling a library function ("handle_transfer") from here
              val libRef = veProcess.load(veFunction.libraryPath.getParent.resolve("sources").resolve(LibCyclone.FileName))
              val batch = withInvocationMetrics(VE) {
                vectorEngine.executeTransfer(libRef, transferDescriptor)
              }

              Seq(batch).iterator
            })
          }
        }
    } else {
      val schema = child.output
      val targetBatchSize = sparkContext.getConf
        .getOption("spark.cyclone.ve.columnBatchSize")
        .map(_.toInt)
        .getOrElse(conf.columnBatchSize)

      child.execute().mapPartitions { internalRows =>
        new Iterator[VeColBatch]{
          private val maxRows = targetBatchSize

          override def hasNext: Boolean = internalRows.hasNext

          override def next(): VeColBatch = {
            withInvocationMetrics(PLAN){
              var curRows = 0
              val descriptor = RowCollectingTransferDescriptor(schema, maxRows)
              withInvocationMetrics("Materialization") {
                while (internalRows.hasNext && curRows < maxRows) {
                  descriptor.append(internalRows.next())
                  curRows += 1
                }
              }

              withInvocationMetrics("Conversion"){
                descriptor.buffer
              }

              // TODO: find a better way of calling a library function ("handle_transfer") from here
              val batch = withInvocationMetrics(VE) {
                val libRef = veProcess.load(veFunction.libraryPath.getParent.resolve("sources").resolve(LibCyclone.FileName))
                vectorEngine.executeTransfer(libRef, descriptor)
              }

              collectBatchMetrics(OUTPUT, batch)
            }
          }
        }
      }
    }
  }

  override def veFunction: VeFunction = parentVeFunction

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(parentVeFunction = f(parentVeFunction))
}
