import com.nec.native.CppTranspiler
import org.apache.spark._
import org.scalatest.freespec.AnyFreeSpec

import scala.reflect.runtime.universe.reify

final class RDDSetupTest extends AnyFreeSpec {

  val conf = new SparkConf().setAppName("Test 01").setMaster("local")
  val sc = new SparkContext(conf)

  "RDDs should work without cluster" - {

    "test a simple RDD map function on local Spark" in {

      val numbers = Array(1,2,3,4,5,6)
      val rdd = sc.parallelize(numbers)
      val rdd2 = rdd.map( x => 2*x )
      val result = rdd2.collect()
      assert(result.sameElements(Array(2,4,6,8,10,12)))
    }

    "test our very own vemap function on local Spark" in {
      /*import com.nec.ve.VeRDD._

      val numbers = (1L to 6L)
      val rdd = sc.veParallelize(numbers)
      val rdd2 = rdd.map((x: Long) => 2 * x)
      val result = rdd2.collect()
      assert(result.sameElements(Array(2L,4L,6L,8L,10L,12L)))

       */
    }

    "eval groupBy" in {
      val code = CppTranspiler.transpileGroupBy(reify { (a: Long) => a % 2 })
      println(code)

      assert(code != null)
    }
  }
}
