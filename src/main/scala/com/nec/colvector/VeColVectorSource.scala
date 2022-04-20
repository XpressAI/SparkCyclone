package com.nec.colvector

final case class VeColVectorSource(identifier: String)

object VeColVectorSource {
  // def make(implicit fullName: sourcecode.FullName, line: sourcecode.Line): VeColVectorSource = {
  //   VeColVectorSource(s"${fullName.value}#${line.value}")
  // }

  object Automatic {
    implicit def newVeColVectorSource(implicit name: sourcecode.FullName,
                                      line: sourcecode.Line): VeColVectorSource = {
      VeColVectorSource(s"${name.value}#${line.value}")
    }
  }
}
