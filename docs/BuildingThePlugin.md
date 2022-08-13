# Building the Spark Cyclone Plugin

## Software Setup

### JDK

The project uses **Java 11**, and Zulu OpenJDK is a preferred JDK.  Users can
switch to the specific JDK every time they work with this repository by installing
[SDKMAN](https://sdkman.io/usage) and running the following command in the project
root directory:

```sh
sdk env
```

### `sbt`

The project uses `sbt` **v1.7.+** for building and running tests.  Users can
download `sbt` by following the instructions [here](https://www.scala-sbt.org/1.x/docs/Setup.html).

### Hadoop + Spark

The plugin is built and tested against **Spark v3.3.0** and **Hadoop v3.3.+**,
respectively.  The instructions for setting up a Hadoop and Spark installation
on a machine with VEs attached can be found on the
project website, [here]](https://sparkcyclone.io/docs/spark-sql/getting-started/hadoop-and-spark-installation-guide)
and [here](https://sparkcyclone.io/docs/spark-sql/getting-started/spark-cyclone-setup).

In addition, instructions for configuring a local (custom) installation of Spark
with an established Hadoop cluster can be found
[here](https://www.linode.com/docs/guides/install-configure-run-spark-on-top-of-hadoop-yarn-cluster/).

#### Hadoop Setup on Windows

For Windows, make sure you configure Hadoop as per
[Hadoop on Windows](https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems)
and set the appropriate `HADOOP_HOME` (Use [winutils](https://github.com/steveloughran/winutils) as needed)

The files should look like this:

```sh
C:/hadoop-3.2.1/bin/hadoop.dll
...
```

Also add the bin directory to the `PATH`.

### Other Setup

#### Cluster Tests

For cluster-mode/detection tests that run on the `VectorEngine` scope, make sure
that `$SPARK_HOME/work` is writable:

```sh
$ mkdir -p /opt/spark/work && chmod -R 777 /opt/spark/work
```

#### SSH

Instructions can be found [here](https://docs.rackspace.com/blog/speeding-up-ssh-session-creation/)
to lower the latency of SSH connections, which is likely needed in the case of
software development involving VEs in a remote server(in general, a 40% decrease
latency can be observed).


## Building and Running

### `sbt Options

The `sbt` console should be launched with large amount of heap memory available:

```sh
SBT_OPTS="-Xmx16g" sbt
```

### Building the Plugin JAR

To build the plugin, simply run in the `sbt` console:

```sbt
show assembly
```

The location of the assembled fat JAR will be displayed.


### Deploying the JAR

A shortcut is provided in the `sbt` console to copy the built plugin JAR to a
pre-determined directory in the filesystem:

```sbt
// Copy the JAR to /opt/cyclone/${USER}/
deploy
```

## Testing the Plugin

See [Testing and CI](./TestingAndCI.md) for more information on how to run Spark
Cyclone tests on different levels.
