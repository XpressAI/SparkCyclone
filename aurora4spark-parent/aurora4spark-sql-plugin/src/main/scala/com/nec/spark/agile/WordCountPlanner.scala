package com.nec.spark.agile

import com.nec.WordCount
import com.nec.spark.agile.WordCountPlanner.WordCounter
import org.apache.arrow.vector.{VarCharVector, VectorSchemaRoot}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.unsafe.types.UTF8String
import com.nec.spark.Aurora4SparkExecutorPlugin

object WordCountPlanner {
  def transformPlan(sparkPlan: SparkPlan): Option[WordCounter => SparkPlan] =
    PartialFunction.condOpt(sparkPlan) {
      case hae @ HashAggregateExec(
            requiredChildDistributionExpressions,
            groupingExpressions,
            Seq(AggregateExpression(Count(_), mode, isDistinct, filter, resultId)),
            aggregateAttributes,
            initialInputBufferOffset,
            resultExpressions,
            ShuffleExchangeExec(
              outputPartitioning,
              HashAggregateExec(
                requiredChildDistributionExpressions2,
                groupingExpressions2,
                aggregateExpressions2,
                aggregateAttributes2,
                initialInputBufferOffset2,
                resultExpressions2,
                child
              ),
              shuffleOrigin
            )
          )
          if groupingExpressions2 == groupingExpressions && groupingExpressions.size == 1 && child.schema.head.dataType == StringType =>
        wordCounter =>
          WordCountPlanner(
            childPlan = child,
            output = resultExpressions.map(_.toAttribute),
            wordCounter = wordCounter
          )
    }

  def apply(sparkPlan: SparkPlan, wordCounter: WordCounter): SparkPlan = {
    sparkPlan.transform(Function.unlift(transformPlan).andThen(_.apply(wordCounter)))
  }

  def applyMaybe(sparkPlan: SparkPlan, wordCounter: WordCounter): Option[SparkPlan] = {
    val resulting =
      sparkPlan.transform(Function.unlift(transformPlan).andThen(_.apply(wordCounter)))
    if (sparkPlan == resulting) None else Some(resulting)
  }

  object WordCounter {

    /** This is the reduce stage to combine all the partitions */
    def combine(a: Map[String, Long], b: Map[String, Long]): Map[String, Long] =
      (a.keySet ++ b.keySet).map(key => key -> (a.getOrElse(key, 0L) + b.getOrElse(key, 0L))).toMap

    object PlainJVM extends WordCounter {
      override def countWords(strings: VarCharVector): Map[String, Long] = {
        (0 until strings.getValueCount)
          .map(idx => Map(new String(strings.get(idx)) -> 1L))
          .reduceOption(combine)
          .getOrElse(Map.empty)
      }
    }

    object VEBased extends WordCounter {
      override def countWords(strings: VarCharVector): Map[String, Long] = {
        WordCount
          .wordCountArrowVE(
            Aurora4SparkExecutorPlugin._veo_proc,
            Aurora4SparkExecutorPlugin._veo_ctx,
            Aurora4SparkExecutorPlugin.lib,
            strings
          )(_.toMap(strings))
          .mapValues(_.toLong)
      }
    }

  }

  trait WordCounter extends Serializable {
    def countWords(strings: VarCharVector): Map[String, Long]
  }
}

case class WordCountPlanner(childPlan: SparkPlan, output: Seq[Attribute], wordCounter: WordCounter)
  extends SparkPlan {
  override protected def doExecute(): RDD[InternalRow] = {
    childPlan
      .execute()
      .mapPartitions { partitionInternalRows =>
        Iterator
          .continually {
            val timeZoneId = conf.sessionLocalTimeZone
            val allocator = ArrowUtilsExposed.rootAllocator.newChildAllocator(
              s"writer for word count",
              0,
              Long.MaxValue
            )
            val arrowSchema = ArrowUtilsExposed.toArrowSchema(schema, timeZoneId)
            val root = VectorSchemaRoot.create(arrowSchema, allocator)
            val arrowWriter = ArrowWriter.create(root)
            val rr =
              try {
                partitionInternalRows.foreach(row => arrowWriter.write(row))
                arrowWriter.finish()
                val res = wordCounter.countWords(root.getVector(0).asInstanceOf[VarCharVector])
                res
              } finally arrowWriter.reset()
            rr
          }
          .take(1)
      }
      .coalesce(1)
      .mapPartitions(iter => Iterator.continually(iter.reduce(WordCounter.combine)).take(1))
      .flatMap { map =>
        map.toList.map { case (v, c) =>
          new GenericInternalRow(Array[Any](UTF8String.fromString(v), c))
        }
      }
  }

  override def children: Seq[SparkPlan] = Seq(childPlan)

}
