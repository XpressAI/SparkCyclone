package io.sparkcyclone.spark.codegen.sort

import io.sparkcyclone.spark.codegen.CFunctionGeneration.TypedCExpression2
import org.apache.spark.sql.catalyst.expressions.{NullOrdering, SortDirection}

final case class VeSortExpression(expression: TypedCExpression2,
                                  direction: SortDirection,
                                  nullOrdering: NullOrdering)
