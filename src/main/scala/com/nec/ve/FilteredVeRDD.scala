package com.nec.ve

import com.nec.spark.agile.core.CFunction2
import com.nec.spark.agile.CFunctionGeneration.CVector
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class FilteredVeRDD[T: ClassTag](prev: VeRDD[T], func: CFunction2, soPath: String, outputs: List[CVector])
  extends VeRDD[T](prev) {

  // TODO: filter
  override def getVectorData(): RDD[VeColBatch] = prev.getVectorData()

}
