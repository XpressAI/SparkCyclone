package com.nec.spark.agile

import com.nec.spark.agile.WordCountPlanner.WordCounter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

object WordCountPlanner {
  def transformPlan(sparkPlan: SparkPlan): Option[SparkPlan] =
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
        WordCountPlanner(
          childPlan = child,
          output = resultExpressions.map(_.toAttribute),
          wordCounter = WordCounter.PlainJVM
        )
    }

  def apply(sparkPlan: SparkPlan): SparkPlan = {
    sparkPlan.transform(Function.unlift(transformPlan))
  }

  object WordCounter {

    /** This is the reduce stage to combine all the partitions */
    def combine(a: Map[String, Long], b: Map[String, Long]): Map[String, Long] =
      (a.keySet ++ b.keySet).map(key => key -> (a.getOrElse(key, 0L) + b.getOrElse(key, 0L))).toMap

    object PlainJVM extends WordCounter {
      override def countWords(strings: List[String]): Map[String, Long] =
        strings.map(str => Map(str -> 1L)).reduceOption(combine).getOrElse(Map.empty)
    }

  }

  trait WordCounter extends Serializable {
    def countWords(strings: List[String]): Map[String, Long]
  }
}

case class WordCountPlanner(childPlan: SparkPlan, output: Seq[Attribute], wordCounter: WordCounter)
  extends SparkPlan {
  override protected def doExecute(): RDD[InternalRow] = {
    childPlan
      .execute()
      .map { ir => ir.getString(0) }
      .mapPartitions(wordsIterator =>
        Iterator.continually(wordCounter.countWords(wordsIterator.toList)).take(1)
      )
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
