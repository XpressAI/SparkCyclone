# aurora4spark-sql-plugin

Requirements:

- SBT
- Hadoop
- JDK 8

## SBT commands

- Unit tests: `~testQuick`
- Functional tests using CMake: `~ CMake / testQuick`
- Functional tests on VE (run on a VH): `~ VectorEngine / testQuick`
- Functional tests on JVM: `testQuick`
- Acceptance tests, which also generate `../FEATURES.md`: `AcceptanceTest / test` (or `Acc`)
- Check before committing: `check` (checks scalafmt and runs any outstanding unit tests)

### Benchmarks

```
~ VectorEngine / runMain com.nec.ve.VeBenchmarkApp
```

Run a specific benchmark:

```
VectorEngine / runMain com.nec.ve.VeBenchmarkApp sum
```

#### JMH
In order to use Spark Rapids for benchmark You need to invoke the following command:
```
export CUDF_PATH=/opt/aurora4spark/cudf-0.19.2-cuda10-1.jar
```
```
jmh:run -prof jmh.extras.JFR -t1 -f 1 -wi 1 -i 1 .*VEJMHBenchmark.*
```

## Currently supported queries

List of currently supported and tested queries can be found [in this file](../FEATURES.md).

## Produce the deployable JAR

```
> show assembly
```

This will show the location of a newly produced `.jar` with the necessary dependencies.

### Deploy the key parts to an environment

```
> deploy
> deployExamples
```

Will upload the `.jar` file and the example `.py` file.

To deploy without running unit tests:

```
> ; set assembly / test := {}; deploy a6; deployExamples a6
```

To deploy to `a5`, do:

```
> ;deploy a5; deployExamples a5
```

### Faster testing over SSH (around 40%) & general log-in to any SSH server

https://docs.rackspace.com/blog/speeding-up-ssh-session-creation/

# Other things

## Frovedis

> Frovedis is high-performance middleware for data analytics. It is written in C++ and utilizes MPI for communication between the servers.

Found here: https://github.com/frovedis/frovedis

Frovedis is the nearest family to what we are trying to do because it has a Spark-VE integration.

For exploration and possible integration purposes, we include the JAR file from the Frovedis distribution for our
reference.

What is also interesting is that Frovedis has an x86 mode which could be tremendously useful for our development.
However, we need to investigate more to see how they have done things and what we can adopt.

### Docker container

To do some exploration we have a Docker container

In order to repeat this, we use Docker:

```
$ cd docker
$ docker build -t frov:1 -f Dockerfile .
$ docker run -it frov:1
```

### JAR repository

We've built some JAR files from the Frovedis sources, so they can be easily consumed from the plug-in and browsed
through IntelliJ's powerful navigation capabilities. This is to aid exploration of what is available. The repository is
located in `frovedis-ivy`
and is available through a default import of SBT. It includes both source and test JARs.

