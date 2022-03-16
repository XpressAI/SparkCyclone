package com.nec.spark.agile.sort

import com.nec.spark.agile.CFunctionGeneration.TypedCExpression2
import org.apache.spark.sql.catalyst.expressions.{NullOrdering, SortDirection}

final case class VeSortExpression(expression: TypedCExpression2,
                                  direction: SortDirection,
                                  nullOrdering: NullOrdering)
