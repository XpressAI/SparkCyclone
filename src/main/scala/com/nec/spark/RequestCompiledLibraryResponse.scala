package com.nec.spark

import okio.ByteString

final case class RequestCompiledLibraryResponse(byteString: ByteString) extends Serializable {

}
