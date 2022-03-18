import com.nec.ve.VeRDD
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

object RDDBench {

  var sc: SparkContext = _
  var timings: Map[String, Double] = Map()

  def main(args: Array[String]): Unit = {
    import com.nec.ve.VeRDD.toVectorizedRDD

    println("RDD Benchmark")

    println("setup")
    sc = setup()

    println("Making numbers")
    val numbers = (1 to (1000000)).toArray
    val doubles = numbers.map( _.asInstanceOf[Double] * Math.PI)

    val rdd = sc.parallelize(numbers).repartition(8)
    val drdd = sc.parallelize(doubles).repartition(8)

    println("Making VeRDD")
    val verdd = toVectorizedRDD(rdd)
    val dverdd = toVectorizedRDD(drdd)

    println("Starting Benchmark")
    benchmark("01 - CPU",  () => bench01cpu(rdd))
    benchmark("01 - VE ", () => bench01ve(verdd))

    // doubles
    benchmark("02 - CPU",  () => bench02cpu(drdd))
    benchmark("02 - VE ", () => bench02ve(dverdd))

    dumpResult()

    //Thread.sleep(300 * 1000)

    finishUp()
  }

  def benchmark(title: String, f: () => Unit): Unit = {
    val ts_start = System.nanoTime()
    f()
    val ts_end = System.nanoTime()
    val diff = (ts_end - ts_start) / 1000000000.0
    timings = timings + (title -> diff)
  }

  def dumpResult(): Unit = {
    println("======== RESULTS ===========")
    for ( (title, duration) <- timings) {
      println(title + "\t" + duration + "seconds")
    }
    println("============================")
  }


  def bench01cpu(rdd: RDD[Int]): Unit = {

    val mappedRdd = rdd.map( (a) => 2 * a + 12)
    val result = mappedRdd.reduce( (a,b) => a + b)

    println("result of bench01 is " + result)

  }

  def bench01ve(rdd: VeRDD[Int]): Unit = {
    import scala.reflect.runtime.universe._

    val expr = reify( (a: Int) => 2 * a + 12)
    val mappedRdd = rdd.vemap(expr)
    val result = mappedRdd.vereduce(reify((a: Int,b: Int) => a + b))

    println("result of bench01 is " + result)
  }


  def bench02cpu(rdd: RDD[Double]): Unit = {

    val mappedRdd = rdd.map( (a) => 2.14 * a + 17.4)
    val result = mappedRdd.reduce( (a,b) => a + b)

    println("result of bench02 is " + result)
  }

  def bench02ve(rdd: VeRDD[Double]): Unit = {
    import scala.reflect.runtime.universe._

    val expr = reify( (a: Double) => 2.14 * a + 17.4)
    val mappedRdd = rdd.vemap(expr)
    val result = mappedRdd.reduce( (a,b) => a + b)

    println("result of bench02 is " + result)
  }

  def setup(): SparkContext = {
    val conf = new SparkConf()
      .setAppName("RDDBench")

    new SparkContext(conf)
  }

  def finishUp(): Unit = {
    sc.stop()
  }




}
