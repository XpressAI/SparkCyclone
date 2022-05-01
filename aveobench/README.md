# AVEO Benchmark

The AVEO Benchmark project is used to design and benchmark different strategies
for moving data between the host and VE.

## Developement

### Writing the Benchmarks

See `com.nec.cyclone.benchmarks.ExampleBenchmarks` for examples in writing
parameterized benchmarks.

### Running the Benchmarks

From the root project directory, run the command in the `sbt` console to kick
off a specific benchmark class:

```sh
aveobench / clean; aveobench / jmh:run -bm avgt -tu ns ExampleBenchmarks
```

This will measure the average time of the code being run, in nanoseconds.  By
default, 1 JVM process fork is made to run 5 warmup iterations followed by 5
production iterations for every benchmark - all three parameters can be adjusted
either on the code level with annotations or on the CLI invocation level with
flags.  Information on all available flags for the `jmh` command can be found in:

```sh
aveobench / jmh:run -h
```

### Benchmark Visualization

To generate reports for visualization, run `jmh` with the following flags:

```sh
-rff benchmark-out.csv -rf csv
```

This will generate a CSV in the `aveobench` project directory, which can then be
imported into a spreadsheet software for custom chart generation.  JSON format
is also supported, and JMH JSON reports can be imported into
[JMH Visualizer](https://jmh.morethan.io/) for quick visualization.

## Resources

* [JMH Visualizer](https://jmh.morethan.io/)
* [JMH Tutorial](https://jenkov.com/tutorials/java-performance/jmh.html)
* [JMH Benchmarking with Scala](https://www.gaurgaurav.com/java/scala-benchmarking-jmh/)
* [JMH Benchmarking with Examples](https://javadevcentral.com/jmh-benchmark-with-examples)
