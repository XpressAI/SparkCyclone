import org.scalatest.freespec.AnyFreeSpec
import org.apache.spark._

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
      import com.nec.ve.VectorizedRDD._
      import scala.reflect.runtime.universe._

      val numbers = Array(1,2,3,4,5,6)
      val rdd = sc.parallelize(numbers)
      val rdd2 = rdd.vemap( reify( (x: Int) => 2*x ) )
      val result = rdd2.collect()
      assert(result.sameElements(Array(1,2,3,4,5,6))) // Note: verdd currently returns its input

    }
  }


}
