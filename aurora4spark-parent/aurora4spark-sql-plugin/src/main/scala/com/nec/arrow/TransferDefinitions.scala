package com.nec.arrow

object TransferDefinitions {

  val TransferDefinitionsSourceCode: String = {
    val source =
      scala.io.Source.fromInputStream(getClass.getResourceAsStream("functions/cpp/transfer-definitions.h"))
    try source.mkString
    finally source.close()
  }

}
