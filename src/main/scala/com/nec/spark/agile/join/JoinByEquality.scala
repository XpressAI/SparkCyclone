package com.nec.spark.agile.join

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{
  CFunction,
  CScalarVector,
  CVarChar,
  CVector,
  VeScalarType
}
import com.nec.spark.agile.groupby.GroupByOutline.initializeScalarVector
import com.nec.spark.agile.join.GenericJoiner.{
  computeNumJoin,
  computeStringJoin,
  EqualityPairing,
  Join
}
import com.nec.spark.agile.join.JoinByEquality.Conjunction

final case class JoinByEquality(
  inputsLeft: List[CVector],
  inputsRight: List[CVector],
  joins: List[Join]
) {

  def pairings: List[EqualityPairing] = joins.map {
    case Join(CVarChar(left), CVarChar(right)) =>
      EqualityPairing(
        indexOfFirstColumn = s"index_${left}",
        indexOfSecondColumn = s"index_${right}"
      )

    case Join(CScalarVector(left, _), CScalarVector(right, _)) =>
      EqualityPairing(
        indexOfFirstColumn = s"index_${left}",
        indexOfSecondColumn = s"index_${right}"
      )
    case other => sys.error(s"Unmatched => $other")
  }

  val out_left = CVector.int("left_idx")

  val out_right = CVector.int("right_idx")

  def ioWo: List[CVector] = inputsLeft ++ inputsRight
  def ioO: List[CVector] = List(out_left, out_right)
  def io: List[CVector] = ioWo ++ ioO

  def produceIndices: CFunction = CFunction(
    inputs = ioWo,
    outputs = ioO,
    body = CodeLines.from(
      ioWo.map { vec =>
        CodeLines.debugValue(s""""${vec.name}->count = """", s"${vec.name}->count")
      },
      joins.map {
        case Join(CVarChar(left), CVarChar(right)) =>
          val pairing = EqualityPairing(
            indexOfFirstColumn = s"index_${left}",
            indexOfSecondColumn = s"index_${right}"
          )
          val left_words = s"${left}_words"
          val left_dict = s"${left}_dict"
          val left_dict_indices = s"${left}_dict_indices"
          CodeLines.from(
            CodeLines.debugHere,
            CodeLines
              .from(
                s"frovedis::words ${left_words} = varchar_vector_to_words(${left});",
                s"frovedis::dict ${left_dict} = frovedis::make_dict(${left_words});"
              ),
            computeStringJoin(
              leftDictIndices = left_dict_indices,
              outMatchingIndicesLeft = pairing.indexOfFirstColumn,
              outMatchingIndicesRight = pairing.indexOfSecondColumn,
              inLeftDict = left_dict,
              inLeftWords = left_words,
              inRightVarChar = right
            )
          )
        case Join(CScalarVector(left, _), CScalarVector(right, _)) =>
          val pairing = EqualityPairing(
            indexOfFirstColumn = s"index_${left}",
            indexOfSecondColumn = s"index_${right}"
          )
          computeNumJoin(
            outMatchingIndicesLeft = pairing.indexOfFirstColumn,
            outMatchingIndicesRight = pairing.indexOfSecondColumn,
            inLeft = left,
            inRight = right
          )
        case other =>
          sys.error(s"Unmatched join: ${other}")
      },
      pairings match {
        case one :: Nil =>
          CodeLines.from(
            List("left", "right").zip(List(one.indexOfFirstColumn, one.indexOfSecondColumn)).map {
              case (nme, in) =>
                CodeLines.from(
                  CodeLines.debugHere,
                  initializeScalarVector(
                    VeScalarType.veNullableInt,
                    s"${nme}_idx",
                    s"${in}.size()"
                  ),
                  CodeLines.debugHere,
                  CodeLines.forLoop("i", s"${in}.size()")(
                    CodeLines.from(
                      s"${nme}_idx->data[i] = ${in}[i];",
                      s"set_validity(${nme}_idx->validityBuffer, i, 1);"
                    )
                  )
                )
            }
          )

        case moreThanOne =>
          CodeLines.from(
            s"std::vector<size_t> left_idx_vec;",
            s"std::vector<size_t> right_idx_vec;",
            Conjunction(moreThanOne).compute("left_idx_vec", "right_idx_vec"),
            CodeLines.from(List("left", "right").zip(List("left_idx_vec", "right_idx_vec")).map {
              case (nme, in) =>
                CodeLines.from(
                  initializeScalarVector(
                    VeScalarType.veNullableInt,
                    s"${nme}_idx",
                    s"${in}.size()"
                  ),
                  CodeLines.forLoop("i", s"${in}.size()")(
                    CodeLines.from(
                      s"${nme}_idx->data[i] = ${in}[i];",
                      s"set_validity(${nme}_idx->validityBuffer, i, 1);"
                    )
                  )
                )
            })
          )
      }
    )
  )

}

object JoinByEquality {

  def nestLoop(loops: List[String], inside: CodeLines): CodeLines =
    loops.foldRight(inside) { case (loopInfo, insideCode) =>
      CodeLines.from(s"for ( $loopInfo ) {", insideCode.indented, "}")
    }

  final case class Conjunction(pairings: List[EqualityPairing]) {
    def loops: List[String] =
      pairings.zip(pairingIndices).map { case (ep, idx) =>
        s"int ${idx} = 0; ${idx} < ${ep.indexOfFirstColumn}.size(); ${idx}++"
      }

    def compute(leftOut: String, rightOut: String): CodeLines =
      nestLoop(
        loops,
        CodeLines.ifStatement(condition)(
          CodeLines.from(
            s"$leftOut.push_back(${pairings.head.indexOfFirstColumn}[${pairingIndices.head}]);",
            s"$rightOut.push_back(${pairings.head.indexOfSecondColumn}[${pairingIndices.head}]);"
          )
        )
      )

    private def pairingIndices: List[String] =
      pairings.indices.map(idx => Character.toString(('i'.toInt + idx).toChar)).toList

    def condition: String = {
      val firstCol = pairings
        .sliding(2)
        .zip(pairingIndices.sliding(2))
        .collect { case (Seq(a, b), Seq(ai, bi)) =>
          s"${a.indexOfFirstColumn}[$ai] == ${b.indexOfFirstColumn}[$bi]"
        }
        .mkString(" && ")

      val secondCol = pairings
        .sliding(2)
        .zip(pairingIndices.sliding(2))
        .collect { case (Seq(a, b), Seq(ai, bi)) =>
          s"${a.indexOfSecondColumn}[$ai] == ${b.indexOfSecondColumn}[$bi]"
        }
        .mkString(" && ")

      s"($firstCol) && ($secondCol)"
    }

  }
}
