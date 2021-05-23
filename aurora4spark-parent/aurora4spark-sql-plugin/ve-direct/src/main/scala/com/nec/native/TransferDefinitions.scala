package com.nec.native

object TransferDefinitions {

  val TransferDefinitionsSourceCode: String = {
    val source =
      scala.io.Source.fromInputStream(getClass.getResourceAsStream("/transfer-definitions.c"))
    try source.mkString
    finally source.close()
  }

}
