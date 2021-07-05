# aurora4spark-sql-plugin

Requirements:

- SBT
- Hadoop
- JDK 8

## Hadoop set up on Windows

For Windows, make sure you configure Hadoop as per [Hadoop on Windows](https://wiki.apache.org/hadoop/WindowsProblems) and set the appropriate `HADOOP_HOME` (download winutils)

Then the files should look like this:

```
C:/hadoop-3.2.1/bin/hadoop.dll
...
```

Also add the bin directory to `PATH`.

## VE configuration options

You can put the following in `~/.bashrc`:

```
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
export PATH=$JAVA_HOME/bin:$PATH
mkdir -p ~/.local/bin
ln -f -s /opt/nec/ve/bin/ncc ~/.local/bin/ncc
ln -f -s /opt/nec/ve/bin/nc++ ~/.local/bin/nc++
ln -f -s /opt/nec/ve/bin/ps ~/.local/bin/veps
source /opt/rh/devtoolset-9/enable
```

## Development for Spark 2.3 / Scala 2.11

Done using SBT's cross-build: https://www.scala-sbt.org/1.x/docs/Cross-Build.html

You can eg create a directory `src/main/scala-2.12` and `src/main/scala-2.11` for compiling those version-specific sources.

As such, any Spark 2.3-specific tests should be done through `src/test/scala-2.11`.

In the shell, switch using `+ 2.11.12`. We tried to use https://github.com/sbt/sbt-projectmatrix - while it worked
it did not gel so well with IntelliJ.

## SBT commands

- Unit tests: `~testQuick`
- Functional tests using CMake: `~ CMake / testQuick` - for this, on Windows use `choco install --force visualstudio2017buildtools` and then install C++ through the Visual Studio Installer.
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
In order to automatically generate and invoke all currently supported benchmarks You should invoke 
the follwing commands: 
```
export CUDF_PATH=/opt/aurora4spark/cudf-0.19.2-cuda10-1.jar
```

```
# get help -- you'll really need to understand these in depth before running benchmarks, else they might be quite meaningless
; skipBenchTests; bench -h; unskipBenchTests

# list benchmarks
; skipBenchTests; bench -h; unskipBenchTests

# run all benchmarks
bench -t1 -f 1 -wi 0 -i 1 .*

# run a specific benchmark with extra options, and also request a flight recording.
# in this case we want to look at the benchmarks to see what is going on at the moment.
bench -f 1 -wi 0 -prof jfr -i 1 .*Parquet.*Rapid.* -to 20m -jvmArgsAppend -Dspark.ui.enabled=true

# to force small data set for whatever reason (eg running on local machine, and don't have the huge data-set), use:  
-jvmArgsAppend -Dnec.testing.force-small=true

bench -f 1 -wi 0 -prof jfr -i 1 -jvmArgsAppend -Dspark.ui.enabled=true .*

```

The first one will set the path to cudf JAR required by rapids benchmarks, while the other one
will generate all benchmarks defined in `BenchTestingPossibilities` class and then start them.

Adding new benchmarks requires implementing `Testing` abstract class and making sure it is included
in `BenchTestingPossibilities.possibilities` list.

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

