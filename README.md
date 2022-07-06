# Spark Cyclone

- For the plug-in development: [SBT.md](SBT.md).
- For the JavaCPP layer around AVEO, see: [https://github.com/bytedeco/javacpp-presets/tree/aurora/veoffload](https://github.com/bytedeco/javacpp-presets/tree/aurora/veoffload).

## Usage of the plugin

Assuming you've deployed the plugin jar file into `/opt/cyclone//`:

```
$ $SPARK_HOME/bin/spark-submit \
    --name YourScript \
    --master yarn \
    --deploy-mode cluster \
    --num-executors=8 --executor-cores=1 --executor-memory=8G \ # specify 1 executor per VE core
    --conf spark.executor.extraClassPath=/opt/cyclone//spark-cyclone-sql-plugin.jar \
    --conf spark.plugins=io.sparkcyclone.spark.AuroraSqlPlugin \
    --jars /opt/cyclone//spark-cyclone-sql-plugin.jar \
    --conf spark.executor.resource.ve.amount=1 \                # specify the number of VEs to use.
    --conf spark.resources.discoveryPlugin=io.sparkcyclone.ve.DiscoverVectorEnginesPlugin
    --conf spark.cyclone.kernel.directory=/opt/cyclone//ccache \ # Place to cache compiled kernels.
    your_script.py

```

## NCC arguments

A good set of NCC defaults is set up, however if further overriding is needed, it can be done with the following Spark
config:

```
--conf spark.cyclone.ncc.path=/opt/nec/ve/bin/ncc
--conf spark.cyclone.ncc.debug=true
--conf spark.cyclone.ncc.o=3
--conf spark.cyclone.ncc.openmp=false
--conf spark.cyclone.ncc.extra-argument.0=-X
--conf spark.cyclone.ncc.extra-argument.1=-Y
```

For safety, if an argument key is not recognized, it will fail to launch.

## Clustering / resource support

A variety of options are available some are necessary for Spark.

## Assinging resources

You must specify vector engines to be used in executors.  There is no need to assign Vector Engines to the driver.

```
--conf spark.executor.resource.ve.amount=1
```

If using cluster-local mode also specify:

```
--conf spark.worker.resource.ve.amount=1
```

Specify this discovery pluging for detecting resources automatically

```
--conf spark.resources.discoveryPlugin=io.sparkcyclone.ve.DiscoverVectorEnginesPlugin
```

Alternatively you can use a script if you want/need more control over which VE is assigned.

```
--conf spark.executor.resource.ve.discoveryScript=/opt/spark/getVEsResources.sh
```

## Compilation lifecycle

The Spark Cyclone plugin will translate your Spark SQL queries into a C++ kernel to execute them on the Vector Engine.
Compilation can take anywhere from a few seconds to a couple minutes.  While insignificant if your queries take hours
you can optimize the compilation time by specifying a directory to cache kernels using the following config.

### Specify a directory to compile and cache kernels

If a suitable kernel exists in the directory, the Spark Cyclone plugin will use it and not compile a new one from
scratch.

```
--conf spark.cyclone.kernel.directory=/path/to/compilation/dir
```

## Batching

This is to batch ColumnarBatch together, to allow for larger input sizes into the VE. This may however use more on-heap
and off-heap memory.

```
--conf spark.cyclone.spark.batch-batches=3
```

Default is 0, which is just to pass ColumnarBatch directly in.

## Pre-shuffling/hashing

This will try to pre-shuffle the data so that we only need to call the VE in one stage for aggregations. It might be
more performant due to avoiding a coalesce/shuffle afterwards.

```
--conf spark.cyclone.spark.preshuffle-partitions=8
```

## Configuration options

Note, _default specified in the `=`_

```
--conf spark.cyclone.spark.aggregate-on-ve=true
--conf spark.cyclone.spark.sort-on-ve=false
--conf spark.cyclone.spark.project-on-ve=true
--conf spark.cyclone.spark.filter-on-ve=true
--conf spark.cyclone.spark.exchange-on-ve=true
--conf spark.cyclone.spark.pass-through-project=false
--conf spark.cyclone.spark.fail-fast=false
--conf spark.cyclone.spark.join-on-ve=false
--conf spark.cyclone.spark.amplify-batches=true
--conf spark.cyclone.ve.columnBatchSize=<spark col batch size>
--conf spark.cyclone.ve.targetBatchSizeMb=64
```

## Benchmarking

- [tpcbench-run/README.md](tpcbench-run/README.md)
