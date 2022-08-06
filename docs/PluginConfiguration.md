# Spark Cyclone Configuration Options

The default options are generally specified after the `=` in the examples below
unless otherwise noted.


## Logging Options

### Set Logging Level

To enable logs for the plugin, the following lines need to be added to
`$SPARK_HOME/conf/log4j2.properties`:

```sh
logger.sparkcyclone.name = io.sparkcyclone
logger.sparkcyclone.level = TRACE
```

In addition, AVEO logs can be enabled by adding the following Spark
configuration flag:

```sh
--conf spark.executorEnv.VEO_LOG_DEBUG=1
```

### Collecting JVM Crashes

Due to the fragile interop between C++ and JVM that is inherent in the plugin,
it may be possible to encounter bugs that manifest as JVM crashes.  To be able
to collect JVM crash logs deterministically, the following Spark configuration
flag needs to be added:

```sh
--conf spark.executor.extraJavaOptions="-XX:ErrorFile=/tmp/hs_err_pid%p.log"
```

In this example, the crash log will be available in `/tmp/`.


## Resources Options

### Resource Discovery

Spark Cyclone's VE discovery plugin must be specified for automatically
detecting VE resources:

```sh
--conf spark.resources.discoveryPlugin=io.sparkcyclone.plugin.DiscoverVectorEnginesPlugin
```

Alternatively, if more control over the VE assignment is desired, a custom
script can be supplied:

```sh
--conf spark.executor.resource.ve.discoveryScript=/opt/spark/getVEsResources.sh
```

### Assigning Resources

VEs must be specified as a resource in the Spark executors, though there is no
need to assign VEs to the driver:

```sh
--conf spark.executor.resource.ve.amount=1
```

If the Spark installation is running in cluster-local mode, also specify the
following:

```sh
--conf spark.worker.resource.ve.amount=1
```

Each VE has 8 physical cores available and by default, all 8 are used. To limit
the number of cores assigned per VE process:

```sh
--conf spark.cyclone.resource.ve.cores=4
```

However, as of time of writing, AVEO or (JavaCPP's wrapper thereof) appear to
support only a **1:1** mapping between a VE process and a physical VE (attempting
to spin up a second VE process on a given node will fail after hanging).  This
means that `spark.executor.resource.ve.amount` should be set to the same  value
as `--num-executors` to avoid VE resource starvation as the job is running.

In addition, it is recommended that the number of CPU cores assigned per executor
should be set such that `num-executors x executor-cores` is roughly 90% of all
CPU cores available in the system.  Using a machine with 2 physical VEs and 48
CPU cores as an example, the following configuration is recommended:

```sh
--num-executors=2
--executor-cores=20
--executor-memory=8G
--conf spark.executor.resource.ve.amount=2
```

Here, there will be 2 executors, each running 20 CPU threads, where group of 20
threads will be sharing a VE process managing 8 VE thread contexts.

## Spark SQL Options

### Execution on VE

The following configurations determine how the SQL query tree is transformed,
such that certain SQL plan nodes are executed on the VE while the rest are
executed on the CPU:

```sh
--conf spark.cyclone.sql.aggregate-on-ve=true       # Enable column aggregations
--conf spark.cyclone.sql.sort-on-ve=false           # Enable sort
--conf spark.cyclone.sql.project-on-ve=true         # Enable value projections
--conf spark.cyclone.sql.filter-on-ve=true          # Enable column filters
--conf spark.cyclone.sql.exchange-on-ve=true        # Enable exchange
--conf spark.cyclone.sql.join-on-ve=false           # Enable joins on the VE
--conf spark.cyclone.sql.pass-through-project=false # Pass through for projections
--conf spark.cyclone.sql.fail-fast=false            # Fail the query execution if an exception is thrown while transforming the query tree (instead of skipping)
--conf spark.cyclone.sql.amplify-batches=true       # Amplify batches
```

### Batch Configuration and Tuning

Data is transferred into the VE via VE column batches, and the size of these
batches will affect the efficiency of the data transfer.  The following
configurations are used to tune the VE column batch sizes.

```sh
--conf spark.cyclone.ve.columnBatchSize=128000  # The target number of rows in the VE column batch
--conf spark.cyclone.ve.targetBatchSizeMb=64    # The target data size of the VE column batch
```

In addition, the following standard Spark SQL options may be useful to tune for
optimal performance:

```sh
--conf spark.sql.inMemoryColumnarStorage.batchSize=128000 # Controls the size of batches for columnar caching.  Larger batch sizes can improve memory utilization and compression, but risk OOMs when caching data.
--conf spark.sql.columnVector.offheap.enabled=true        # Enables [[OffHeapColumnVector]] in [[ColumnarBatch]]
--conf spark.sql.autoBroadcastJoinThreshold=-1            # Maximum size (in bytes) for a table that will be broadcast to all worker nodes when performing a join.  Negative values or 0 disable broadcasting.
--conf spark.sql.shuffle.partitions=8                     # Number of partitions to use by default when shuffling data for joins or aggregations
```

Please see the [Spark Internals Book](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-properties.html)
For more tuning options.


## C++ Kernel Compilation Options

### `ncc` Arguments

The plugin's native code compiler is set up with a good set of `ncc` defaults.
However, if custom compiler flags and overrides are needed, they can be added
with the following Spark configuration flags:

```sh
--conf spark.cyclone.ncc.path=/opt/nec/ve/bin/ncc
--conf spark.cyclone.ncc.debug=true
--conf spark.cyclone.ncc.o=3
--conf spark.cyclone.ncc.openmp=false
--conf spark.cyclone.ncc.extra-argument.0=-X
--conf spark.cyclone.ncc.extra-argument.1=-Y
```

For safety, if an argument key is not recognized, an error will be thrown at
runtime.

### Kernel Build Directory

By default, a tempoarry directory will be created on plugin start, which Spark
Cyclone will use to build and cache the C++ kernels it builds.  To specify a
given directory instead, the following configuration can be used:

```
--conf spark.cyclone.kernel.directory=/path/to/compilation/directory
```

If a suitable kernel already exists in the directory, the plugin will use it
instead of compiling a new one from scratch.

The compiled C++ kernels are cached and the cache is persisted in the directory
across Spark job launches.  Furthermore, the kernels are dynamically linked to
the built `libcyclone.so` that comes packaged with the plugin.  This means that
if you're upgrading the plugin version (users), or running a Spark job as part
of making changes to the Cyclone C++ library itself (developers), it is recommended
to clear the directory between upgrades and job launches, respectively, to avoid
possible linkage errors encountered by the plugin during runtime.
