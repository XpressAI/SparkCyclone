# spark-cyclone-sql-plugin

Requirements:

- SBT
- Hadoop
- JDK 8

## Hadoop set up on Windows

For Windows, make sure you configure Hadoop as per [Hadoop on Windows](https://wiki.apache.org/hadoop/WindowsProblems)
and set the appropriate `HADOOP_HOME` (download winutils)

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
export SPARK_SCALA_VERSION=2.12
export SPARK_HOME=/path/to/spark # you might want to download your own too
```

## Cluster tests

For Cluster-mode/detection tests that run on the `VectorEngine` scope, ensure that `$SPARK_HOME/work` is
writable (`mkdir -p /opt/spark/work && chmod -R 777 /opt/spark/work`).

## SBT commands

- Unit tests: `~testQuick`
- Functional tests using CMake: `~ CMake / testQuick` - for this, on Windows
  use `choco install --force visualstudio2017buildtools` and then install C++ through the Visual Studio Installer.
- Functional tests on VE (run on a VH): `~ VectorEngine / testQuick`
- Functional tests on JVM: `testQuick`
- Acceptance tests, which also generate `../FEATURES.md`: `AcceptanceTest / test` (or `Acc`)
- Check before committing: `check` (checks scalafmt and runs any outstanding unit tests)

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

# SBT options

## `failFast`

`set failFast := true` if you'd like it to fail fast in `TPC/test`

`set debugToHtml := true` if you'd like `TPC/test` to output more detail for verbosity (like plans).

# Other things

## Frovedis

> Frovedis is high-performance middleware for data analytics. It is written in C++ and utilizes MPI for communication between the servers.

Found here: https://github.com/frovedis/frovedis

Frovedis is the nearest family to what we are trying to do because it has a Spark-VE integration.

For exploration and possible integration purposes, we include the JAR file from the Frovedis distribution for our
reference.

What is also interesting is that Frovedis has an x86 mode which could be tremendously useful for our development.

However, we need to investigate more to see how they have done things and what we can adopt.

