# Spark Cyclone

[Spark Cyclone Homepage](https://sparkcyclone.io/)

Spark Cyclone is an [Apache Spark](https://spark.apache.org/) plug-in that
accelerates the performance of Spark by using the
[SX-Aurora TSUBASA "Vector Engine" (VE)](https://www.nec.com/en/global/solutions/hpc/sx/vector_engine.html).
The plugin enables Spark users to accelerate their existing jobs by generating
optimized C++ code and executing it on the VE, with minimal or no effort.

Spark Cyclone currently offers three pathways to accelerate Spark on the VE:

* [Spark SQL](https://spark.apache.org/sql/): The plugin leverages Spark SQL's
  extensibility to rewrite SQL queries on the fly and executes dynamically-generated
  C++ code with no user code changes necessary.
* [RDD](https://spark.apache.org/docs/latest/rdd-programming-guide.html): For
  more direct control, the plugin's VERDD API API provides Scala macros that can
  be used to transpile normal Scala code into C++ and thus execute common RDD
  operations such as `map()` on the VE.
* [MLlib](https://spark.apache.org/mllib/):  [CycloneML](https://github.com/XpressAI/CycloneML)
  is a fork of MLLib that uses Spark Cyclone to accelerate many of the ML algorithms
  using either the VE or CPU.


## Plugin Usage

Integrating the Spark Cyclone plugin into an existing Spark job is very straightforward.
The following is the minimum set of flags that need to be added to an existing
Spark job configuration:

```
$ $SPARK_HOME/bin/spark-submit \
    --name YourSparkJobName \
    --master yarn \
    --deploy-mode cluster \
    --num-executors=8 --executor-cores=1 --executor-memory=8G \                                 # Specify 1 executor per VE core
    --jars /path/to/spark-cyclone-sql-plugin.jar \                                              # Add the Spark Cyclone plugin JAR
    --conf spark.executor.extraClassPath=/path/to/spark-cyclone-sql-plugin.jar \                # Add Spark Cyclone libraries to the classpath
    --conf spark.plugins=io.sparkcyclone.plugin.AuroraSqlPlugin \                               # Specify the plugin's main class
    --conf spark.executor.resource.ve.amount=1 \                                                # Specify the number of VEs to use
    --conf spark.resources.discoveryPlugin=io.sparkcyclone.plugin.DiscoverVectorEnginesPlugin \ # Specify the class used to discover VE resources
    --conf spark.cyclone.kernel.directory=/path/to/kernel/directory \                           # Specify a directory where the plugin builds and caches C++ kernels
    YourSparkJob.py
```

### Configuration

Please refer to the [Plugin Configuration Guide](docs/PluginConfiguration.md)
for an overview of the configuration options available to Spark Cyclone.


## Plugin Development

### System Setup

While parts of the codebase can be developed on a standard `x86` machine running
Linux or MacOS, building and testing the plugin requires a system that has VEs
properly installed and set up - please refer to the
[VE Documentation](https://www.hpc.nec/documents/) for more information on this.
The following guides contain all the necessary setup and installation steps:

* [SX-Aurora TSUBASA Installation Guide](https://www.hpc.nec/documents/guide/pdfs/InstallationGuide_E.pdf)
* [SX-Aurora TSUBASA Setup Guide](https://www.hpc.nec/documents/guide/pdfs/SetupGuide_E.pdf)

In particular, the system should have the following software ready after setup:

* [VEOS](https://github.com/veos-sxarr-NEC/veos)
* [AVEO](https://sxauroratsubasa.sakura.ne.jp/documents/veos/en/aveo/index.html)
* [NEC C Compiler (NCC)](https://www.nec.com/en/global/solutions/hpc/sx/tools.html)

### JDK

This project uses **Java 11**, and Zulu OpenJDK is the preferred JDK.  Users can
switch to the specific JDK every time they work with this repository by installing
[SDKMAN](https://sdkman.io/usage) and running the following command in the project
root directory:

```sh
sdk env
```

### Spark + Hadoop

The plugin has been built and tested against **Spark v3.3.0** and **Hadoop v3.3.+**,
respectively.  Instructions for installing and configuring Spark for Hadoop can
be found [here](https://www.linode.com/docs/guides/install-configure-run-spark-on-top-of-hadoop-yarn-cluster/).

### Build and Run

Spark Cyclone is built using [sbt](https://www.scala-sbt.org/).  To build the
plugin, simply run:

```sh
sbt assembly
```

The assembled fat JAR will be found in `target/`.

### Prerequisite Guides

* Usage:
  * [Plugin Configuration](docs/PluginConfiguration.md)

* Internal Development:
  * [C++ Cyclone Library](src/main/resources/io/sparkcyclone/cpp/README.md)
  * [Testing and Continuous Integration](docs/TestingAndCI.md)
  * [Program Flow](docs/ProgramFlow.md)
  * [Expression Evaluation](docs/ExpressionEvaluation.md)
  * [Expression Evaluation](docs/ExpressionEvaluation.md)

* External Dependencies:
  * [Frovedis Library](https://github.com/frovedis/frovedis)
  * [JavaCPP Layer Around AVEO](https://github.com/bytedeco/javacpp-presets/tree/aurora/veoffload)
