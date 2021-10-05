package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.GroupByExpression.{
  GroupByAggregation,
  GroupByProjection
}
import com.nec.spark.agile.CFunctionGeneration.{
  CFunction,
  CScalarVector,
  CVarChar,
  CVector,
  GroupByExpression,
  NamedGroupByExpression,
  NamedStringProducer,
  StringGrouping,
  TypedCExpression2,
  VeGroupBy
}

//noinspection MapFlatten
final case class GroupByFunctionGeneration(
  veDataTransformation: VeGroupBy[
    CVector,
    Either[StringGrouping, TypedCExpression2],
    Either[NamedStringProducer, NamedGroupByExpression]
  ]
) {

  def tuple =
    s"std::tuple<${veDataTransformation.groups
      .flatMap {
        case Right(v) =>
          List(v.veType.cScalarType) ++ v.cExpression.isNotNullCode.map(_ => "int").toList
        case Left(s) =>
          List("long")
      }
      .mkString(", ")}>"

  /**
   * We return: grouping constituents followed by the aggregate partials
   */
  private def partialOutputs
    : List[Either[(NamedStringProducer, CVector), (NamedGroupByExpression, List[CVector])]] = {
    veDataTransformation.outputs.zipWithIndex.map {
      case (
            Right(
              n @ NamedGroupByExpression(outputName, veType, GroupByExpression.GroupByProjection(_))
            ),
            _
          ) =>
        Right(n -> List(CScalarVector(outputName, veType)))
      case (
            Right(
              n @ NamedGroupByExpression(
                outputName,
                veType,
                GroupByExpression.GroupByAggregation(agg)
              )
            ),
            idx
          ) =>
        Right(n -> agg.partialValues(outputName).map(_._1))
      case (e @ Left(n @ NamedStringProducer(outputName, _)), idx) =>
        Left(n -> CVarChar(outputName))
    }
  }

  private def partialOutputVectors: List[CVector] =
    partialOutputs
      .map(_.left.map(_._2).left.map(List.apply(_)))
      .map(_.right.map(_._2))
      .flatMap(_.fold(identity, identity))

  def renderPartialGroupBy: CFunction = {
    val firstInput = veDataTransformation.inputs.head
    val computeGroups = StagedGroupBy.identifyGroups(
      tupleType = tuple,
      groupingVecName = "full_grouping_vec",
      count = s"${firstInput.name}->count",
      thingsToGroup = veDataTransformation.groups.map {
        case Left(StringGrouping(name)) => Left(name)
        case Right(t)                   => Right(t.cExpression)
      },
      groupsCountOutName = "groups_count",
      groupsIndicesName = "groups_indices",
      sortedIdxName = "sorted_idx"
    )

    def computeProjections(
      outputName: String,
      veType: CFunctionGeneration.VeScalarType,
      ex: CFunctionGeneration.CExpression
    ): CodeLines =
      CodeLines.from(
        "",
        CodeLines
          .from(
            CodeLines.debugHere,
            StagedGroupBy.initializeOutputVector(veType, outputName, "groups_count"),
            StagedGroupBy.forHeadOfEachGroup(
              groupsCountName = "groups_count",
              groupsIndicesName = "groups_indices",
              sortedIdxName = "sorted_idx"
            )(StagedGroupBy.storeTo(outputName, ex))
          )
          .blockCommented(s"Output ${outputName}")
      )

    def computeAggregations(
      finalOutputName: String,
      agg: CFunctionGeneration.Aggregation
    ): CodeLines = {
      CodeLines.from(
        "",
        s"// Partials' output for ${finalOutputName}:",
        CodeLines.debugHere,
        agg.partialValues(s"${finalOutputName}").map {
          case (CScalarVector(outputName, partialType), cExpression) =>
            StagedGroupBy.initializeOutputVector(
              veScalarType = partialType,
              outputName = outputName,
              count = "groups_count"
            )
        },
        CodeLines.debugHere,
        StagedGroupBy.forEachGroupItem(
          groupsCountName = "groups_count",
          groupsIndicesName = "groups_indices",
          sortedIdxName = "sorted_idx"
        )(
          beforeFirst = agg.initial(finalOutputName),
          perItem = agg.iterate(finalOutputName),
          afterLast = CodeLines.from(
            agg.compute(finalOutputName),
            "// store the result",
            agg.partialValues(s"${finalOutputName}").map {
              case (CScalarVector(outputName, partialType), cExpression) =>
                StagedGroupBy.storeTo(outputName, cExpression)
            }
          )
        )
      )
    }

    def remapStrings: CodeLines = CodeLines.from(
      partialOutputs
        .flatMap(_.left.toSeq)
        .map(_._1)
        .map { case NamedStringProducer(name, stringProducer) =>
          val fp = StringProducer.FilteringProducer(name, stringProducer)
          CodeLines
            .from(
              fp.setup,
              StagedGroupBy.forHeadOfEachGroup(
                groupsCountName = "groups_count",
                groupsIndicesName = "groups_indices",
                sortedIdxName = "sorted_idx"
              )(fp.forEach),
              fp.complete,
              StagedGroupBy.forHeadOfEachGroup(
                groupsCountName = "groups_count",
                groupsIndicesName = "groups_indices",
                sortedIdxName = "sorted_idx"
              )(fp.validityForEach)
            )
            .blockCommented(s"Produce the string group")
        }
    )

    CFunction(
      inputs = veDataTransformation.inputs,
      outputs = partialOutputVectors,
      body = CodeLines.from(
        CodeLines.debugHere("\"input count\"", s"${firstInput.name}->count"),
        computeGroups,
        "/** perform computations for every output **/",
        CodeLines.debugHere,
        "/** possibly perform a String re-mapping **/",
        remapStrings,
        CodeLines.debugHere("groups_count"),
        CodeLines.from(partialOutputs.flatMap(_.right.toSeq.map(_._1)).map {
          case NamedGroupByExpression(outputName, veType, GroupByProjection(ex)) =>
            computeProjections(outputName, veType, ex)
          case NamedGroupByExpression(finalOutputName, veType, GroupByAggregation(agg)) =>
            computeAggregations(finalOutputName, agg)
        })
      )
    )
  }

  private def partialInputs
    : List[Either[(NamedStringProducer, CVector), (NamedGroupByExpression, List[CVector])]] =
    partialOutputs.map {
      case Left((nsp, cv)) => Left((nsp, cv.replaceName("output", "input")))
      case Right((ng, lv)) => Right((ng, lv.map(_.replaceName("output", "input"))))
    }

  def partialInputVectors: List[CVector] =
    partialOutputVectors.map(_.replaceName("output", "input"))

  def renderFinalGroupBy: CFunction = {
    val firstInput = partialInputVectors.head

    def identifyGroups: CodeLines =
      StagedGroupBy.identifyGroups(
        tupleType = tuple,
        groupingVecName = "full_grouping_vec",
        count = s"${firstInput.name}->count",
        thingsToGroup = veDataTransformation.groups.map {
          case Left(StringGrouping(name)) => Left(name)
          case Right(tc)                  => Right(tc.cExpression)
        },
        groupsCountOutName = "groups_count",
        groupsIndicesName = "groups_indices",
        sortedIdxName = "sorted_idx"
      )

    def performComputations: CodeLines =
      CodeLines.from(
        partialInputs
          .flatMap(_.left.toSeq)
          .map(_._1)
          .map { case NamedStringProducer(name, stringProducer) =>
            val fp = StringProducer.FilteringProducer(name, stringProducer)
            CodeLines
              .from(
                fp.setup,
                "// for each group",
                CodeLines.debugHere,
                StagedGroupBy.forHeadOfEachGroup(
                  groupsCountName = "groups_count",
                  groupsIndicesName = "groups_indices",
                  sortedIdxName = "sorted_idx"
                )(fp.validityForEach)
              )
              .blockCommented(s"Produce the string group")
          }
      )

    def completeAggregations(
      outputName: String,
      veType: CFunctionGeneration.VeScalarType,
      agg: CFunctionGeneration.Aggregation
    ): CodeLines =
      CodeLines.from(
        CodeLines.debugHere,
        StagedGroupBy.initializeOutputVector(veType, outputName, "groups_count"),
        StagedGroupBy.forEachGroupItem(
          groupsCountName = "groups_count",
          groupsIndicesName = "groups_indices",
          sortedIdxName = "sorted_idx"
        )(
          beforeFirst = agg.initial(outputName),
          perItem = agg.merge(outputName, outputName.replaceAllLiterally("output", "input")),
          afterLast = CodeLines.from(
            agg.compute(outputName),
            StagedGroupBy.storeTo(outputName, agg.fetch(outputName)),
            agg.free(outputName)
          )
        )
      )

    def completeProjections(
      outputName: String,
      veType: CFunctionGeneration.VeScalarType,
      ex: CFunctionGeneration.CExpression
    ): CodeLines =
      CodeLines.from(
        CodeLines.debugHere,
        StagedGroupBy.initializeOutputVector(veType, outputName, "groups_count"),
        StagedGroupBy.forHeadOfEachGroup(
          groupsCountName = "groups_count",
          groupsIndicesName = "groups_indices",
          sortedIdxName = "sorted_idx"
        )(StagedGroupBy.storeTo(outputName, ex))
      )

    CFunction(
      inputs = partialInputVectors,
      outputs = renderGroupBy.outputs,
      body = CodeLines.from(
        identifyGroups,
        CodeLines.debugHere,
        "/** perform computations for every output **/",
        performComputations,
        CodeLines.from(partialInputs.flatMap(_.right.toSeq).map {
          case (NamedGroupByExpression(outputName, veType, GroupByAggregation(agg)), vecs) =>
            completeAggregations(outputName, veType, agg)
          case (NamedGroupByExpression(outputName, veType, GroupByProjection(ex)), _) =>
            completeProjections(outputName, veType, ex)
        })
      )
    )
  }

  def renderGroupBy: CFunction = {
    val firstInput = veDataTransformation.inputs.head
    CFunction(
      inputs = veDataTransformation.inputs,
      outputs = veDataTransformation.outputs.zipWithIndex.map {
        case (Right(NamedGroupByExpression(outputName, veType, _)), idx) =>
          CScalarVector(outputName, veType)
        case (Left(NamedStringProducer(outputName, _)), idx) =>
          CVarChar(outputName)
      },
      body = CodeLines.from(
        StagedGroupBy.identifyGroups(
          tupleType = tuple,
          groupingVecName = "full_grouping_vec",
          count = s"${firstInput.name}->count",
          thingsToGroup = veDataTransformation.groups.map {
            case Left(StringGrouping(name)) => Left(name)
            case Right(tp) =>
              Right(tp.cExpression)
          },
          groupsCountOutName = "groups_count",
          groupsIndicesName = "groups_indices",
          sortedIdxName = "sorted_idx"
        ),
        "/** perform computations for every output **/",
        veDataTransformation.outputs.zipWithIndex.map {
          case (Left(NamedStringProducer(name, stringProducer)), idx) =>
            val fp = StringProducer.FilteringProducer(name, stringProducer)
            CodeLines
              .from(
                fp.setup,
                "// for each group",
                StagedGroupBy.forHeadOfEachGroup(
                  groupsCountName = "groups_count",
                  groupsIndicesName = "groups_indices",
                  sortedIdxName = "sorted_idx"
                )(fp.forEach),
                fp.complete,
                StagedGroupBy.forHeadOfEachGroup(
                  groupsCountName = "groups_count",
                  groupsIndicesName = "groups_indices",
                  sortedIdxName = "sorted_idx"
                )(fp.validityForEach)
              )
              .blockCommented(s"Produce the string group")
          case (Right(NamedGroupByExpression(outputName, veType, groupByExpr)), idx) =>
            CodeLines.from(
              StagedGroupBy.initializeOutputVector(
                veScalarType = veType,
                outputName = outputName,
                count = "groups_count"
              ),
              StagedGroupBy.forEachGroupItem(
                groupsCountName = "groups_count",
                groupsIndicesName = "groups_indices",
                sortedIdxName = "sorted_idx"
              )(
                beforeFirst = groupByExpr
                  .fold(whenProj = _ => CodeLines.empty, whenAgg = agg => agg.initial(outputName)),
                perItem = groupByExpr
                  .fold(whenProj = _ => CodeLines.empty, whenAgg = _.iterate(outputName)),
                afterLast = CodeLines.from(
                  groupByExpr.fold(_ => CodeLines.empty, whenAgg = _.compute(outputName)),
                  "// store the result",
                  StagedGroupBy.storeTo(
                    outputName,
                    groupByExpr.fold(whenProj = ce => ce, whenAgg = _.fetch(outputName))
                  ),
                  groupByExpr.fold(_ => CodeLines.empty, _.free(outputName))
                )
              )
            )
        }
      )
    )
  }

}
