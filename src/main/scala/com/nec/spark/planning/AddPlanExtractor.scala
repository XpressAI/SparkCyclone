package com.nec.spark.planning
import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.spark.agile.PairwiseAdditionOffHeap
import com.nec.spark.planning.SparkPortingUtils.{PortedSparkPlan, RowToColumnarExec}

import org.apache.spark.sql.catalyst.expressions.{Add, Alias}
import org.apache.spark.sql.execution.{LocalTableScanExec, ProjectExec, SparkPlan}

/**
 * Basic SparkPlan matcher that will match a plan that sums a bunch of BigDecimals, and gets them
 * from the Local Spark table.
 *
 * This is done so that we have something basic to work with.
 */
object AddPlanExtractor {

  def matchAddPairwisePlan(
    sparkPlan: SparkPlan,
    arrowInterface: ArrowNativeInterfaceNumeric
  ): Option[SparkPlan] = {

    /**
     * We cannot force LocalTableScanExec to be done on the VEO because at this point already the
     * computation has been pushed down - at the point of [[LocalTableScanExec]].
     */
    PartialFunction
      .condOpt(sparkPlan) { case pe @ ProjectExec(Seq(Alias(Add(_, _), name)), child) =>
        PairwiseAdditionOffHeap(
          if (child.supportsColumnar) child else RowToColumnarExec(child),
          arrowInterface
        )
      }
  }

}
