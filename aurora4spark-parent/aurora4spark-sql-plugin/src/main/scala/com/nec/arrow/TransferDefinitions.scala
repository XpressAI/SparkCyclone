package com.nec.arrow
import com.nec.spark.agile.CppResource

object TransferDefinitions {

  val TransferDefinitionsSourceCode: String = {
    CppResource("cpp/transfer-definitions.h").readString
  }

}
