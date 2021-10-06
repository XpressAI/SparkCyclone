package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.GroupByExpression.{
  GroupByAggregation,
  GroupByProjection
}
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.StagedGroupBy.{
  GroupingCodeGenerator,
  GroupingKey,
  StagedAggregation,
  StagedAggregationAttribute,
  StagedProjection
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
        case Left(_) => List("long")
      }
      .mkString(", ")}>"

  def codeGenerator: GroupingCodeGenerator = GroupingCodeGenerator(
    groupingVecName = "full_grouping_vec",
    groupsCountOutName = "groups_count",
    groupsIndicesName = "groups_indices",
    sortedIdxName = "sorted_idx"
  )

  def renderGroupBy: CFunction = {
    val stagedGroupBy = StagedGroupBy(
      groupingKeys = veDataTransformation.groups.zipWithIndex.map {
        case (Left(stringGrouping), i) => GroupingKey(s"g_${i}", VeString)
        case (Right(typedExp), i)      => GroupingKey(s"g_${i}", typedExp.veType)
      },
      finalOutputs = veDataTransformation.outputs.zipWithIndex.map {
        case (
              Right(NamedGroupByExpression(outputName, veType, GroupByAggregation(aggregation))),
              idx
            ) =>
          Right(
            Right(
              StagedAggregation(
                outputName,
                veType,
                aggregation.partialValues(outputName).map { case (cv, ce) =>
                  StagedAggregationAttribute(name = cv.name, veType)
                }
              )
            )
          )
        case (
              Right(NamedGroupByExpression(outputName, veType, GroupByProjection(cExpression))),
              idx
            ) if veDataTransformation.groups.exists(_.right.exists(_.cExpression == cExpression)) =>
          Left(GroupingKey(outputName, veType))
        case (
              Right(NamedGroupByExpression(outputName, veType, GroupByProjection(cExpression))),
              idx
            ) =>
          Right(Left(StagedProjection(outputName, veType)))
        case (Left(NamedStringProducer(outputName, _)), idx)
            if veDataTransformation.groups.exists(_.left.exists(_.name == outputName)) =>
          Left(GroupingKey(outputName, VeString))
        case (Left(NamedStringProducer(outputName, _)), idx) =>
          Right(Left(StagedProjection(outputName, VeString)))
      }
    )

    val pf = stagedGroupBy.createPartial(
      inputs = veDataTransformation.inputs,
      computeGroupingKey = ???,
      computeProjection = ???,
      computeAggregate = ???
    )

    val ff = stagedGroupBy.createFinal(
      projectionIsString = ???,
      groupingKeyIsString = ???,
      computeAggregate = ???
    )

    CFunction(
      inputs = pf.inputs,
      outputs = ff.outputs,
      body = CodeLines.from(
        pf.outputs.map(cv => StagedGroupBy.declare(cv)),
        pf.body.blockCommented("Partial stage"),
        ff.body.blockCommented("Final stage"),
        pf.outputs.map(cv => StagedGroupBy.dealloc(cv))
      )
    )

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
        codeGenerator.identifyGroups(
          tupleType = tuple,
          count = s"${firstInput.name}->count",
          thingsToGroup = veDataTransformation.groups.map {
            case Left(StringGrouping(name)) => Left(name)
            case Right(tp) =>
              Right(tp.cExpression)
          }
        ),
        "/** perform computations for every output **/",
        veDataTransformation.outputs.zipWithIndex.map {
          case (Left(NamedStringProducer(name, stringProducer)), idx) =>
            val fp = StringProducer.FilteringProducer(name, stringProducer)
            CodeLines
              .from(
                fp.setup,
                "// for each group",
                codeGenerator.forHeadOfEachGroup(fp.forEach),
                fp.complete,
                codeGenerator.forHeadOfEachGroup(fp.validityForEach("g"))
              )
              .blockCommented(s"Produce the string group")
          case (Right(NamedGroupByExpression(outputName, veType, groupByExpr)), idx) =>
            CodeLines.from(
              StagedGroupBy.initializeScalarVector(
                veScalarType = veType,
                variableName = outputName,
                countExpression = "groups_count"
              ),
              codeGenerator.forEachGroupItem(
                beforeFirst = groupByExpr
                  .fold(whenProj = _ => CodeLines.empty, whenAgg = agg => agg.initial(outputName)),
                perItem = groupByExpr
                  .fold(whenProj = _ => CodeLines.empty, whenAgg = _.iterate(outputName)),
                afterLast = CodeLines.from(
                  groupByExpr.fold(_ => CodeLines.empty, whenAgg = _.compute(outputName)),
                  "// store the result",
                  StagedGroupBy.storeTo(
                    outputName,
                    groupByExpr.fold(whenProj = ce => ce, whenAgg = _.fetch(outputName)),
                    idx = "g"
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
