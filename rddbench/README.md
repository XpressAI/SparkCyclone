# RDD Benchmark

A utility to benchmark RDD performance, with and without VeRDD.

# Quickstart

From project directory run:
```
sbt assembly
sbt rddbench/package
cd rddbench
./run_rddbench.sh
```

# Console

To get started with RDDs on the console, try the following:
```
$sbt
sbt:spark-cyclone-sql-plugin> project rddbench
sbt:rddbench> console
...
scala> import org.apache.spark._
scala> val conf = new SparkConf().setAppName("Test").setMaster("local")
scala> val sc = new SparkContext(conf)
scala> val numbers = Array(1,2,3,4,5)
scala> val rdd = sc.parallelize(numbers)
scala> val rdd2 = rdd.map( (x) => 2*x )
scala> rdd2.collect.foreach(println)
```

