package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.GroupByComputation.{
  Aggregable,
  Copiable,
  Groupable,
  Inputtable,
  OutputDescription,
  Outputtable,
  Projectable
}

import scala.language.higherKinds

final case class GroupByComputation[Output, Grouping, Projection, Aggregate](
  /** List of final outputs in their intended order */
  outputs: List[OutputDescription[Output, Grouping, Projection, Aggregate]],
  /** Group-By keys to represent the situation where a grouping is not being outputted */
  grouping: List[Grouping]
) {

  /** ensure that all the groupings we refer to are indeed valid */
  require {
    grouping.forall(grouping =>
      outputs.view
        .map(_.source)
        .collect { case Left(g) => g }
        .contains(grouping)
    )
  }

  def mapOutput[O2](f: Output => O2): GroupByComputation[O2, Grouping, Projection, Aggregate] =
    copy(outputs = outputs.map(_.mapOutput(f)))
  def mapGrouping[G2](f: Grouping => G2): GroupByComputation[Output, G2, Projection, Aggregate] =
    copy(outputs = outputs.map(_.mapGrouping(f)), grouping = grouping.map(f))
  def mapProjection[P2](f: Projection => P2): GroupByComputation[Output, Grouping, P2, Aggregate] =
    copy(outputs = outputs.map(_.mapProjection(f)))
  def mapAggregate[A2](f: Aggregate => A2): GroupByComputation[Output, Grouping, Projection, A2] =
    copy(outputs = outputs.map(_.mapAggregate(f)))

  case class Produce[Inputs](inputs: Inputs)(implicit
    groupable: Groupable[Grouping],
    aggregable: Aggregable[Aggregate]
  ) {
    def producePartial(implicit
      projectable: Projectable[Projection],
      inputtable: Inputtable[Inputs],
      og: Outputtable[Grouping],
      op: Outputtable[Projection],
      oa: Outputtable[Aggregate]
    ): CFunction = {
      CFunction(
        inputs = inputtable.names(inputs),
        outputs = List(
          grouping.flatMap(og.names),
          outputs.flatMap(_.source.right.toSeq).flatMap(_.left.toSeq).flatMap(op.names),
          outputs.flatMap(_.source.right.toSeq).flatMap(_.right.toSeq).flatMap(oa.names)
        ).flatten,
        body = CodeLines.from(
          groupable.groupInitial(grouping),
          outputs.flatMap(_.source.right.toSeq).flatMap(_.left.toSeq).map(projectable.project),
          outputs
            .flatMap(_.source.right.toSeq)
            .flatMap(_.right.toSeq)
            .map(aggregable.computePartials)
        )
      )
    }

    def produceFinal(implicit copiable: Copiable[Projection], o: Outputtable[Output]): CFunction = {
      CFunction(
        inputs = Nil,
        outputs = outputs.map(_.output).flatMap(o.names),
        CodeLines.from(
          groupable.groupFinal(grouping),
          outputs.flatMap(_.source.right.toSeq).flatMap(_.left.toSeq).map(copiable.copy),
          outputs.flatMap(_.source.right.toSeq).flatMap(_.right.toSeq).map(aggregable.computeFinals)
        )
      )
    }
  }

}

object GroupByComputation {

  trait Aggregable[Aggregate] {
    def computePartials(a: Aggregate): CodeLines
    def computeFinals(a: Aggregate): CodeLines
  }

  trait Inputtable[X] {
    def names(x: X): List[CVector]
  }

  trait Outputtable[X] {
    def names(x: X): List[CVector]
  }

  trait Copiable[X] {
    def copy(x: X): CodeLines
  }

  trait Projectable[Projection] {
    def project(projection: Projection): CodeLines
  }

  trait Groupable[Grouping] {
    def groupInitial(grouping: List[Grouping]): CodeLines
    def groupFinal(grouping: List[Grouping]): CodeLines
  }

  /**
   * Partial computation :=
   *  - Output keys that are used in Grouping
   *  - Outputs that are not used in Grouping
   *  - Grouping keys that are not in output
   *  - Aggregate attributes
   */

  final case class OutputDescription[O, G, P, A](output: O, source: Either[G, Either[P, A]]) {
    def mapOutput[O2](f: O => O2): OutputDescription[O2, G, P, A] = copy(output = f(output))
    def mapGrouping[G2](f: G => G2): OutputDescription[O, G2, P, A] =
      copy(source = source.left.map(f))
    def mapAggregate[A2](f: A => A2): OutputDescription[O, G, P, A2] =
      copy(source = source.right.map(_.map(f)))
    def mapProjection[P2](f: P => P2): OutputDescription[O, G, P2, A] =
      copy(source = source.right.map(_.left.map(f)))
  }

}
