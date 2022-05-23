/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.sparkcyclone.spark.codegen.groupby

import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin
import io.sparkcyclone.spark.codegen.CFunctionGeneration.{Aggregation, CFunction, TypedCExpression2}
import io.sparkcyclone.spark.codegen.StringHole.StringHoleEvaluation
import io.sparkcyclone.spark.codegen.groupby.GroupByOutline._
import io.sparkcyclone.native.code.CFunction2.CFunctionArgument
import io.sparkcyclone.native.code._

final case class GroupByPartialGenerator(
  finalGenerator: GroupByPartialToFinalGenerator,
  computedGroupingKeys: Seq[(GroupingKey, Either[StringReference, TypedCExpression2])],
  computedProjections: Seq[(StagedProjection, Either[StringReference, TypedCExpression2])],
  stringVectorComputations: Seq[StringHoleEvaluation],
  nBuckets: Int = SparkCycloneExecutorPlugin.totalVeCores
) {
  import finalGenerator._
  import stagedGroupBy._

  val BatchAssignmentsId = "batch_assignments"
  val BatchGroupPositionsId = "batch_group_positions"
  val BatchCountsId = "batch_counts"

  def createPartial(name: String, inputs: Seq[CVector]): PartialAggregateFunction = {
    val arguments = {
      inputs
        .map { v => v.withNewName(s"${v.name}_m") }
        .map(CFunctionArgument.PointerPointer(_)) ++
      Seq(CFunctionArgument.Raw("int* sets")) ++
      partialOutputs
        .map(CFunctionArgument.PointerPointer(_))
    }

    val body = CodeLines.from(
      inputs.map { v =>
        s"${v.veType.cVectorType} *${v.name} = ${v.name}_m[0];"
      },
      allocateOutputBatchPointers,
      performGrouping(count = s"${inputs.head.name}->count"),
      computeBatchPlacementsPerGroup,
      countBatchSizes,
      allocateActualBatches,
      stringVectorComputations.map(_.computeVector),
      computeGroupingKeysPerGroup,
      computedProjections.map { case (sp, e) =>
        computeProjectionsPerGroup(sp, e)
      },
      computedAggregates.map { case (a, ag) =>
        computeAggregatePartialsPerGroup(a, ag)
      },
      freeGroupingAllocations
    )

    PartialAggregateFunction(
      name,
      partialOutputs,
      CFunction2(name, arguments, body)
    )
  }

  /**
   * Allocate output as batches and set output batch count
   */
  private def allocateOutputBatchPointers: CodeLines = {
    CodeLines.from(
      partialOutputs.map( v =>
        s"*${v.name} = static_cast<${v.veType.cVectorType} *>(malloc(sizeof(nullptr) * ${nBuckets}));"
      ),
      "",
      "// Write out the number of batches",
      s"sets[0] = ${nBuckets};",
      "",
    )
  }

  def performGrouping(count: String): CodeLines = {
    CodeLines.from(
      groupingCodeGenerator.identifyGroups(
        tupleTypes = tupleTypes,
        tupleType = tupleType,
        count = count,
        thingsToGroup = computedGroupingKeys.map { case (_, e) =>
          e.map(_.cExpression).left.map(_.name)
        }
      ),
      "",
      s"std::vector<size_t> matching_ids(${groupingCodeGenerator.groupsCountOutName});",
      s"size_t* matching_ids_arr = matching_ids.data();",
      "#pragma _NEC vector",
      CodeLines.forLoop("g", groupingCodeGenerator.groupsCountOutName) {
        s"matching_ids_arr[g] = ${groupingCodeGenerator.sortedIdxName}[${groupingCodeGenerator.groupsIndicesName}[g]];"
      },
      ""
    )
  }

  def computeBatchPlacementsPerGroup: CodeLines = {
    CodeLines.from(
      s"std::vector<size_t> ${BatchAssignmentsId}(${groupingCodeGenerator.groupsCountOutName});",
      CodeLines.scoped("Compute batch placement per group"){
        CodeLines.from("#pragma _NEC vector",
          groupingCodeGenerator.forHeadOfEachGroup(
            CodeLines.from(
              // Initialize the hash
              s"int64_t hash = 1;",
              // Compute the hash across all keys
              computedGroupingKeys.map{
                case (_, Right(TypedCExpression2(_, cExp))) =>
                  s"hash = 31 * hash + (${cExp.cCode});"
                case (_, Left(StringReference(name))) =>
                  CodeLines.forLoop("j", s"${name}->count") {
                    s"hash = ${name}->hash_at(j, hash);"
                  }.cCode
              },
              // Assign the bucket based on the hash
              s"${BatchAssignmentsId}[g] = __builtin_abs(hash % ${nBuckets});"
            ))
        )
      },
      ""
    )
  }

  def countBatchSizes: CodeLines = {
    CodeLines.from(
      s"std::vector<size_t> ${BatchCountsId}(${nBuckets});",
      s"std::vector<size_t> ${BatchGroupPositionsId}(${groupingCodeGenerator.groupsCountOutName});",
      CodeLines.scoped("Compute the value counts for each batch") {
        CodeLines.from(
          "#pragma _NEC vector",
          CodeLines.forLoop("b", s"${nBuckets}") {
            CodeLines.from(
              s"size_t count = 0;",
              // Count the assignments that equal g
              CodeLines.forLoop("g", groupingCodeGenerator.groupsCountOutName) {
                CodeLines.ifStatement(s"${BatchAssignmentsId}[g] == b"){
                  CodeLines.from(
                    s"${BatchGroupPositionsId}[g] = count;",
                    s"count++;"
                  )
                }
              },
              // Assign to the counts table
              s"${BatchCountsId}[b] = count;"
            )
          }
        )
      },
      ""
    )
  }

  def allocateActualBatches: CodeLines = {
    CodeLines.from(
      "// Allocate actual batches to be filled with aggregation results",
      partialOutputs.map{ cVector =>
        CodeLines.forLoop("b", s"$nBuckets"){
          val accessor = s"${cVector.name}[b]"
          CodeLines.from(
            s"$accessor = ${cVector.veType.cVectorType}::allocate();",
            if (cVector.veType != VeString) {
              s"if(${BatchCountsId}[b] != 0) $accessor->resize(${BatchCountsId}[b]);"
            } else CodeLines.empty,
          )
        }
      },
      ""
    )
  }

  def computeGroupingKeysPerGroup: CodeLines = {
    final case class ProductionTriplet(forEach: CodeLines, complete: CodeLines)
    val initVars = computedGroupingKeys.map {
      case (groupingKey, Right(TypedCExpression2(_, cExp))) =>
        ProductionTriplet(
          forEach = storeTo(s"partial_${groupingKey.name}[${BatchAssignmentsId}[g]]", cExp, s"${BatchGroupPositionsId}[g]"),
          complete = CodeLines.empty
        )
      case (groupingKey, Left(StringReference(sr))) =>
        ProductionTriplet(
          forEach = CodeLines.from(
            s"partial_str_${groupingKey.name}[${BatchAssignmentsId}[g]]->move_assign_from(${sr}->select(matching_ids));"
          ),
          complete = CodeLines.empty
        )
    }

    CodeLines.scoped("Compute grouping keys per group") {
      CodeLines.from(
        groupingCodeGenerator.forHeadOfEachGroup(initVars.map(_.forEach)),
        initVars.map(_.complete)
      )
    }
  }

  def computeProjectionsPerGroup(
    stagedProjection: StagedProjection,
    r: Either[StringReference, TypedCExpression2]
  ): CodeLines = r match {
    case Left(StringReference(sr)) =>
      CodeLines.from(s"partial_str_${stagedProjection.name}->move_assign_from(${sr}->select(matching_ids));")
    case Right(TypedCExpression2(veType, cExpression)) =>
      CodeLines.from(
        groupingCodeGenerator.forHeadOfEachGroup(
          storeTo(s"partial_${stagedProjection.name}[${BatchAssignmentsId}[g]]", cExpression, s"${BatchGroupPositionsId}[g]")
        )
      )
  }

  def computeAggregatePartialsPerGroup(
    stagedAggregation: StagedAggregation,
    aggregate: Aggregation
  ): CodeLines = {
    val prefix = s"partial_${stagedAggregation.name}"
    CodeLines.from(
      groupingCodeGenerator.forEachGroupItem(
        beforeFirst = aggregate.initial(prefix),
        perItem = aggregate.iterate(prefix),
        afterLast =
          CodeLines.from(stagedAggregation.attributes.zip(aggregate.partialValues(prefix)).map {
            case (attr, (_, ex)) =>
              CodeLines.from(
                storeTo(s"partial_${attr.name}[${BatchAssignmentsId}[g]]", ex, s"${BatchGroupPositionsId}[g]")
              )
          })
      ),
      ""
    )
  }

  def freeGroupingAllocations: CodeLines = {
    CodeLines.from(
      "free(sorted_idx);",
      "free(groups_indices);"
    )
  }
}
