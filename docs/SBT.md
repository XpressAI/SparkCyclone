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

### Faster testing over SSH (around 40%) & general log-in to any SSH server

https://docs.rackspace.com/blog/speeding-up-ssh-session-creation/
