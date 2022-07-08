package io.sparkcyclone.spark.codegen.groupby

import io.sparkcyclone.spark.codegen.CFunctionGeneration._
import io.sparkcyclone.spark.codegen.core._
import io.sparkcyclone.spark.codegen.groupby.GroupByOutline.{GroupingKey, StagedAggregation, StagedProjection}

/*
  NOTE: These VeFunctionTemplates are currently skeletons, and are intended to
  host more code over time as they are migrated over from the groupby code
  generation routines.
*/

final case class PartialAggregateFunction(name: String,
                                          outputs: Seq[CVector],
                                          func: CFunction2) extends VeFunctionTemplate {
  def hashId: Int = {
    (getClass.getName, outputs.map(_.veType), func.body).hashCode
  }

  def toCFunction: CFunction2 = {
    func
  }

  def secondary: Seq[CFunction2] = {
    Seq.empty
  }
}

final case class FinalAggregateFunction(name: String,
                                        outputs: Seq[CVector],
                                        func: CFunction2) extends VeFunctionTemplate {
  def hashId: Int = {
    (getClass.getName, outputs.map(_.veType), func.body).hashCode
  }

  def toCFunction: CFunction2 = {
    func
  }

  def secondary: Seq[CFunction2] = {
    Seq.empty
  }
}
