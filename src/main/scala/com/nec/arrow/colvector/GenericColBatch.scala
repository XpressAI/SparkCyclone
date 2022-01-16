package com.nec.arrow.colvector

final case class GenericColBatch[Data](numRows: Int, cols: List[Data]) {
  def nonEmpty: Boolean = numRows > 0

  def map[To](f: Data => To): GenericColBatch[To] = copy(cols = cols.map(f))
}
