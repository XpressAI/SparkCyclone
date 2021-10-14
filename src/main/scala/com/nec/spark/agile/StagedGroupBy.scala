package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{
  Aggregation,
  CExpression,
  CFunction,
  CVector,
  VeScalarType,
  VeString,
  VeType
}
import com.nec.spark.agile.StagedGroupBy.{
  storeTo,
  GroupingCodeGenerator,
  GroupingKey,
  StagedAggregation,
  StagedProjection,
  StringReference
}
import com.nec.spark.agile.StringProducer.{CopyStringProducer, FilteringProducer}

final case class StagedGroupBy(
  groupingKeys: List[GroupingKey],
  finalOutputs: List[Either[StagedProjection, StagedAggregation]]
) {

  def createFull(
    inputs: List[CVector],
    computeGroupingKey: GroupingKey => Option[Either[StringReference, CExpression]],
    computeProjection: StagedProjection => Option[Either[StringReference, CExpression]],
    computeAggregate: StagedAggregation => Option[Aggregation]
  ): CFunction = {

    val pf = createPartial(inputs, computeGroupingKey, computeProjection, computeAggregate)
    val ff = createFinal(computeAggregate)
    CFunction(
      inputs = pf.inputs,
      outputs = ff.outputs,
      body = CodeLines.from(
        CodeLines.commentHere(
          "Declare the variables for the output of the Partial stage for the unified function"
        ),
        pf.outputs.map(cv => StagedGroupBy.declare(cv)),
        pf.body.blockCommented("Perform the Partial computation stage"),
        ff.body.blockCommented("Perform the Final computation stage"),
        pf.outputs
          .map(cv => StagedGroupBy.dealloc(cv))
          .blockCommented("Deallocate the partial variables")
      )
    )
  }

  def debugOutputs: CodeLines =
    CodeLines.from(
      CodeLines.commentHere("Debugging finals"),
      finalOutputs.map(_.fold(_.name, _.name)).map { name =>
        StagedGroupBy.debugVector(name)
      }
    )

  def debugPartialOutputs: CodeLines =
    CodeLines.from(
      CodeLines.commentHere("Debugging partials"),
      partialOutputs.map(_.name).map { name =>
        StagedGroupBy.debugVector(name)
      }
    )

  def gcg: GroupingCodeGenerator = GroupingCodeGenerator(
    groupingVecName = "grouping_vec",
    groupsCountOutName = "groups_count",
    groupsIndicesName = "groups_indices",
    sortedIdxName = "sorted_idx"
  )

  def outputs: List[CVector] = finalOutputs.map {
    case Left(stagedProjection) => stagedProjection.veType.makeCVector(stagedProjection.name)
    case Right(stagedAggregation) =>
      stagedAggregation.finalType.makeCVector(stagedAggregation.name)
  }

  def projections: List[StagedProjection] =
    finalOutputs.flatMap(_.left.toSeq)

  def aggregations: List[StagedAggregation] =
    finalOutputs.flatMap(_.right.toSeq)

  private def partialOutputs: List[CVector] = {
    List(
      groupingKeys.map(gk =>
        gk.veType match {
          case VeString => gk.veType.makeCVector(s"partial_str_${gk.name}")
          case other    => other.makeCVector(s"partial_${gk.name}")
        }
      ),
      projections.map(pr =>
        pr.veType.makeCVector(
          if (pr.veType.isString) s"partial_str_${pr.name}" else s"partial_${pr.name}"
        )
      ),
      aggregations.flatMap(agg =>
        agg.attributes.map(att => att.veScalarType.makeCVector(s"partial_${att.name}"))
      )
    ).flatten
  }

  def computeAggregatePartialsPerGroup(
    deriveAggregate: StagedAggregation => Option[Aggregation]
  ): CodeLines = CodeLines.from(
    CodeLines.debugHere,
    CodeLines.commentHere("Compute aggregate partials per group"),
    aggregations.map { stagedAggregation =>
      deriveAggregate(stagedAggregation)
        .map { aggregate =>
          val prefix = s"partial_${stagedAggregation.name}"
          CodeLines.from(
            CodeLines.debugHere,
            stagedAggregation.attributes.map(attribute =>
              StagedGroupBy.initializeScalarVector(
                veScalarType = attribute.veScalarType,
                variableName = s"partial_${attribute.name}",
                countExpression = gcg.groupsCountOutName
              )
            ),
            CodeLines.debugHere,
            gcg.forEachGroupItem(
              beforeFirst = aggregate.initial(prefix),
              perItem = aggregate.iterate(prefix),
              afterLast = CodeLines.from(
                stagedAggregation.attributes.zip(aggregate.partialValues(prefix)).map {
                  case (attr, (vec, ex)) =>
                    CodeLines.from(StagedGroupBy.storeTo(s"partial_${attr.name}", ex, "g"))
                }
              )
            )
          )

        }
        .getOrElse(sys.error(s"Could not match ${stagedAggregation}"))
    }
  )

  def computeProjectionsPerGroup(
    compute: StagedProjection => Option[Either[StringReference, CExpression]]
  ): CodeLines = {
    CodeLines.from(
      CodeLines.debugHere,
      projections.map {
        case sp @ StagedProjection(name, VeString) =>
          compute(sp) match {
            case Some(Left(StringReference(sourceName))) =>
              val fp = FilteringProducer(s"partial_str_${name}", CopyStringProducer(sourceName))
              CodeLines.from(
                CodeLines.debugHere,
                fp.setup,
                gcg.forHeadOfEachGroup(
                  CodeLines.from(
                    //                CodeLines.debugExpr(CExpression(cCode = s"output_0->count", None)),
                    //                CodeLines.debugExpr(CExpression(cCode = s"groups_count", None)),
                    fp.forEach
                  )
                ),
                fp.complete,
                gcg.forHeadOfEachGroup(CodeLines.from(fp.validityForEach("g")))
              )
            case other => sys.error(s"Could not produce for ${sp}; got ${other}")
          }
        case sp @ StagedProjection(name, veType: VeScalarType) =>
          compute(sp) match {
            case Some(Right(cExpression)) =>
              CodeLines.from(
                CodeLines.debugHere,
                StagedGroupBy
                  .initializeScalarVector(veType, s"partial_${name}", gcg.groupsCountOutName),
                gcg.forHeadOfEachGroup(StagedGroupBy.storeTo(s"partial_${name}", cExpression, "g"))
              )
            case other => sys.error(s"Could not map ${sp}, got ${other}")
          }
      }
    )
  }

  def createPartial(
    inputs: List[CVector],
    computeGroupingKey: GroupingKey => Option[Either[StringReference, CExpression]],
    computeProjection: StagedProjection => Option[Either[StringReference, CExpression]],
    computeAggregate: StagedAggregation => Option[Aggregation]
  ): CFunction =
    CFunction(
      inputs = inputs,
      outputs = partialOutputs,
      body = {
        CodeLines.from(
          performGrouping(count = s"${inputs.head.name}->count", compute = computeGroupingKey),
          computeGroupingKeysPerGroup(computeGroupingKey),
          computeProjectionsPerGroup(computeProjection),
          computeAggregatePartialsPerGroup(computeAggregate)
        )
      }
    )

  def createFinal(computeAggregate: StagedAggregation => Option[Aggregation]): CFunction =
    CFunction(
      inputs = partialOutputs,
      outputs = outputs,
      body = {
        CodeLines.from(
//          debugPartialOutputs,
          performGroupingOnKeys,
          mergeAndProduceAggregatePartialsPerGroup(computeAggregate),
          passProjectionsPerGroup
//          debugOutputs
        )
      }
    )

  def tupleTypes: List[String] =
    groupingKeys
      .flatMap { groupingKey =>
        groupingKey.veType match {
          case vst: VeScalarType => List(vst.cScalarType, "int")
          case VeString          => List("long")
        }
      }

  def tupleType: String =
    tupleTypes.mkString(start = "std::tuple<", sep = ", ", end = ">")

  def performGrouping(
    count: String,
    compute: GroupingKey => Option[Either[StringReference, CExpression]]
  ): CodeLines =
    CodeLines.debugHere ++ gcg.identifyGroups(
      tupleTypes = tupleTypes,
      tupleType = tupleType,
      count = count,
      thingsToGroup = groupingKeys.map(gk =>
        compute(gk) match {
          case Some(Left(StringReference(name))) => Left(name)
          case Some(Right(cExpression))          => Right(cExpression)
          case None                              => sys.error(s"Could not match ${gk}")
        }
      )
    )

  def performGroupingOnKeys: CodeLines =
    CodeLines.from(
      CodeLines.debugHere("partial_group_0->count"),
      gcg.identifyGroups(
        tupleTypes = tupleTypes,
        tupleType = tupleType,
        count = s"${partialOutputs.head.name}->count",
        thingsToGroup = groupingKeys.map(gk =>
          gk.veType match {
            case _: VeScalarType =>
              Right(
                CExpression(
                  s"partial_${gk.name}->data[i]",
                  Some(s"check_valid(partial_${gk.name}->validityBuffer, i)")
                )
              )
            case VeString => Left(s"partial_str_${gk.name}")
          }
        )
      )
    )

  def mergeAndProduceAggregatePartialsPerGroup(
    computeAggregate: StagedAggregation => Option[Aggregation]
  ): CodeLines = {
    CodeLines.from(
      CodeLines.debugHere,
      aggregations.map(sa =>
        computeAggregate(sa) match {
          case None => sys.error(s"Could not compute for: ${sa}")
          case Some(aggregation) =>
            CodeLines.from(
              StagedGroupBy.initializeScalarVector(
                veScalarType = sa.finalType.asInstanceOf[VeScalarType],
                variableName = sa.name,
                countExpression = gcg.groupsCountOutName
              ),
              CodeLines.commentHere("producing aggregate/partials per group"),
              gcg.forEachGroupItem(
                beforeFirst = aggregation.initial(sa.name),
                perItem = aggregation.merge(sa.name, s"partial_${sa.name}"),
                afterLast = CodeLines.from(
                  StagedGroupBy.storeTo(sa.name, aggregation.fetch(sa.name), "g")
//                  StagedGroupBy
//                    .debugVector(sa.name)
//                    .blockCommented(s"Debug ${sa.name} from aggregate")
                )
              )
            )
        }
      )
    )
  }

  def passProjectionsPerGroup: CodeLines =
    CodeLines.from(projections.map {
      case StagedProjection(name, VeString) =>
        val fp = FilteringProducer(name, CopyStringProducer(s"partial_str_${name}"))
        CodeLines.from(
          CodeLines.debugHere,
          fp.setup,
          gcg.forHeadOfEachGroup(fp.forEach),
          fp.complete,
          gcg.forHeadOfEachGroup(fp.validityForEach("g"))
        )
      case stagedProjection @ StagedProjection(name, scalarType: VeScalarType) =>
        CodeLines.from(
          StagedGroupBy.initializeScalarVector(
            veScalarType = scalarType,
            variableName = stagedProjection.name,
            countExpression = gcg.groupsCountOutName
          ),
          gcg.forHeadOfEachGroup(
            CodeLines.from(
              StagedGroupBy.storeTo(
                stagedProjection.name,
                CExpression(
                  cCode = s"partial_${stagedProjection.name}->data[i]",
                  isNotNullCode =
                    Some(s"check_valid(partial_${stagedProjection.name}->validityBuffer, i)")
                ),
                "g"
              )
            )
          )
        )
    })

  def computeGroupingKeysPerGroup(
    compute: GroupingKey => Option[Either[StringReference, CExpression]]
  ): CodeLines = CodeLines.from(
    CodeLines.debugHere,
    groupingKeys.map(groupingKey =>
      groupingKey.veType match {
        case scalarType: VeScalarType =>
          StagedGroupBy.initializeScalarVector(
            veScalarType = scalarType,
            variableName = s"partial_${groupingKey.name}",
            countExpression = gcg.groupsCountOutName
          )
        case VeString =>
          val fp =
            FilteringProducer(
              s"partial_str_${groupingKey.name}",
              CopyStringProducer(
                compute(groupingKey).map {
                  case Right(cExp) => "???"
                  case Left(StringReference(name)) => name
                  case _ => "???"
                }.getOrElse("???")
              )
            )
          fp.setup
      }
    ),
    CodeLines.debugHere,
    groupingKeys.map(groupingKey =>
      gcg.forHeadOfEachGroup(CodeLines.from(compute(groupingKey) match {
        case Some(Right(cExp)) =>
          storeTo(s"partial_${groupingKey.name}", cExp, "g")
        case Some(Left(StringReference(name))) =>
          val fp =
            FilteringProducer(s"partial_str_${groupingKey.name}", CopyStringProducer(name))

          CodeLines.from(fp.forEach)
        case other =>
          sys.error(s"Unsupported right now: ${other} (from ${groupingKey})")
      }))
    ),
    CodeLines.debugHere,
    groupingKeys.map(groupingKey =>
      CodeLines.from(compute(groupingKey) match {
        case Some(Left(StringReference(name))) =>
          val fp =
            FilteringProducer(s"partial_str_${groupingKey.name}", CopyStringProducer(name))

          CodeLines.from(fp.complete, gcg.forHeadOfEachGroup(fp.validityForEach("g")))
        case _ => CodeLines.empty
      })
    )
  )

  def passGroupingKeysPerGroup: CodeLines =
    groupingKeys.map(groupingKey =>
      gcg.forHeadOfEachGroup(
        CodeLines.from(
          CodeLines.debugHere,
          storeTo(
            outputName =
              if (groupingKey.veType.isString) s"partial_str_${groupingKey.name}"
              else s"partial_${groupingKey.name}",
            cExpression = CExpression(s"${groupingKey.name}->data[i]", None),
            idx = "g"
          )
        )
      )
    )

}

object StagedGroupBy {
  def initializeStringVector(variableName: String): CodeLines = CodeLines.empty

  def debugVector(name: String): CodeLines = {
    CodeLines.from(
      s"for (int i = 0; i < ${name}->count; i++) {",
      CodeLines.from(
        s"""std::cout << "${name}[" << i << "] = " << ${name}->data[i] << " (valid? " << check_valid(${name}->validityBuffer, i) << ")" << std::endl << std::flush; """
      ),
      "}"
    )
  }

  def dealloc(cv: CVector): CodeLines = CodeLines.empty

  def declare(cv: CVector): CodeLines = CodeLines.from(
    s"${cv.veType.cVectorType} *${cv.name} = (${cv.veType.cVectorType}*)malloc(sizeof(${cv.veType.cVectorType}));"
  )

  final case class StringReference(name: String)
  final case class InputReference(name: String)
  final case class GroupingKey(name: String, veType: VeType)
  final case class StagedProjection(name: String, veType: VeType)
  final case class StagedAggregationAttribute(name: String, veScalarType: VeScalarType)
  final case class StagedAggregation(
    name: String,
    finalType: VeType,
    attributes: List[StagedAggregationAttribute]
  )

  final case class GroupingCodeGenerator(
    groupingVecName: String,
    groupsCountOutName: String,
    groupsIndicesName: String,
    sortedIdxName: String
  ) {

    def identifyGroups(
      tupleTypes: List[String],
      tupleType: String,
      count: String,
      thingsToGroup: List[Either[String, CExpression]]
    ): CodeLines = {
      val stringsToHash: List[String] = thingsToGroup.flatMap(_.left.toSeq)
      CodeLines.from(
        s"std::vector<${tupleType}> ${groupingVecName}(${count});",
        s"std::vector<size_t> ${sortedIdxName}(${count});",
        stringsToHash.map { name =>
          val stringIdToHash = s"${name}_string_id_to_hash"
          val stringHashTmp = s"${name}_string_id_to_hash_tmp"
          CodeLines.from(
            s"std::vector<long> $stringIdToHash(${count});",
            s"for ( long i = 0; i < ${count}; i++ ) {",
            CodeLines
              .from(
                s"long ${stringHashTmp} = 0;",
                s"for ( int q = ${name}->offsets[i]; q < ${name}->offsets[i + 1]; q++ ) {",
                CodeLines
                  .from(s"${stringHashTmp} = 31*${stringHashTmp} + ${name}->data[q];")
                  .indented,
                "}",
                s"$stringIdToHash[i] = ${stringHashTmp};"
              )
              .indented,
            "}"
          )
        },
        CodeLines.debugHere,
        s"for ( long i = 0; i < ${count}; i++ ) {",
        CodeLines
          .from(
            s"${sortedIdxName}[i] = i;",
            s"${groupingVecName}[i] = ${tupleType}(${thingsToGroup
              .flatMap {
                case Right(g) => List(g.cCode, g.isNotNullCode.getOrElse("1"))
                case Left(stringName) =>
                  List(s"${stringName}_string_id_to_hash[i]")
              }
              .mkString(", ")});"
          )
          .indented,
        s"}",
        CodeLines.debugHere,
        tupleTypes.zipWithIndex.reverse.collect { case (t, idx) =>
          CodeLines.from(
            s"{",
            s"std::vector<${t}> temp(${count});",
            s"for ( long i = 0; i < ${count}; i++ ) {",
            CodeLines
              .from(s"temp[i] = std::get<${idx}>(${groupingVecName}[${sortedIdxName}[i]]);")
              .indented,
            s"}",
            s"frovedis::radix_sort(temp.data(), ${sortedIdxName}.data(), temp.size());",
            s"}"
          )
        },
        s"for ( long j = 0; j < ${count}; j++ ) {",
        CodeLines
          .from(
            s"long i = ${sortedIdxName}[j];",
            s"${groupingVecName}[j] = ${tupleType}(${thingsToGroup
              .flatMap {
                case Right(g) => List(g.cCode, g.isNotNullCode.getOrElse("1"))
                case Left(stringName) =>
                  List(s"${stringName}_string_id_to_hash[i]")
              }
              .mkString(", ")});"
          )
          .indented,
        s"}",
        CodeLines.debugHere,
        s"std::vector<size_t> ${groupsIndicesName} = frovedis::set_separate(${groupingVecName});",
        s"int ${groupsCountOutName} = ${groupsIndicesName}.size() - 1;"
      )
    }

    def forHeadOfEachGroup(f: => CodeLines): CodeLines =
      CodeLines
        .from(
          s"for (size_t g = 0; g < ${groupsCountOutName}; g++) {",
          CodeLines
            .from(s"long i = ${sortedIdxName}[${groupsIndicesName}[g]];", f)
            .indented,
          "}"
        )

    def forEachGroupItem(
      beforeFirst: => CodeLines,
      perItem: => CodeLines,
      afterLast: => CodeLines
    ): CodeLines =
      CodeLines.from(
        s"for (size_t g = 0; g < ${groupsCountOutName}; g++) {",
        CodeLines
          .from(
            s"size_t group_start_in_idx = ${groupsIndicesName}[g];",
            s"size_t group_end_in_idx = ${groupsIndicesName}[g + 1];",
            "int i = 0;",
            beforeFirst,
            s"for ( size_t j = group_start_in_idx; j < group_end_in_idx; j++ ) {",
            CodeLines
              .from(s"i = ${sortedIdxName}[j];", perItem)
              .indented,
            "}",
            afterLast
          )
          .indented,
        "}"
      )
  }

  def storeTo(outputName: String, cExpression: CExpression, idx: String): CodeLines =
    cExpression.isNotNullCode match {
      case None =>
        CodeLines.from(
          s"""$outputName->data[g] = ${cExpression.cCode};""",
          s"set_validity($outputName->validityBuffer, ${idx}, 1);"
        )
      case Some(notNullCheck) =>
        CodeLines.from(
          s"if ( $notNullCheck ) {",
          CodeLines
            .from(
              s"""$outputName->data[${idx}] = ${cExpression.cCode};""",
              s"set_validity($outputName->validityBuffer, ${idx}, 1);"
            )
            .indented,
          "} else {",
          CodeLines.from(s"set_validity($outputName->validityBuffer, ${idx}, 0);").indented,
          "}"
        )
    }

  def initializeScalarVector(
    veScalarType: VeScalarType,
    variableName: String,
    countExpression: String
  ): CodeLines =
    CodeLines.from(
      s"$variableName->count = ${countExpression};",
      s"$variableName->data = (${veScalarType.cScalarType}*) malloc($variableName->count * sizeof(${veScalarType.cScalarType}));",
      s"$variableName->validityBuffer = (uint64_t *) malloc(ceil(${countExpression} / 64.0) * sizeof(uint64_t));"
    )

}
