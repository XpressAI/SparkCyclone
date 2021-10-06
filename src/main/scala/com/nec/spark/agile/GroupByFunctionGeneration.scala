package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.StagedGroupBy.GroupingCodeGenerator

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
