package com.nec.spark.planning

import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.spark.agile.{PairwiseAdditionOffHeap, BroadcastJoinSparkPlanDescription}

import org.apache.spark.sql.catalyst.expressions.{Add, Alias}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.execution.{LocalTableScanExec, ProjectExec, RowToColumnarExec, SparkPlan}

/**
 * Basic SparkPlan matcher that will match a plan that sums a bunch of BigDecimals, and gets them
 * from the Local Spark table.
 *
 * This is done so that we have something basic to work with.
 */
object JoinPlanExtractor {

  def matchBroadcastJoinPlan(
                              sparkPlan: SparkPlan
                            ): Option[BroadcastJoinSparkPlanDescription] = {

    /**
     * We cannot force LocalTableScanExec to be done on the VEO because at this point already the
     * computation has been pushed down - at the point of [[LocalTableScanExec]].
     */
    PartialFunction
      .condOpt(sparkPlan) {
        case pe@ProjectExec(
        projectList,
        BroadcastHashJoinExec(
        leftKeys,
        rightKeys,
        joinType,
        buildSide,
        condition,
        left,
        right,
        isNullAwareAntiJoin
        )
        ) => BroadcastJoinSparkPlanDescription(left, right, leftKeys.head, rightKeys.head)
      }

  }
}
