package com.nec.spark.agile.groupby

import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.core._
import com.nec.spark.agile.groupby.GroupByOutline.{GroupingKey, StagedAggregation, StagedProjection}

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
