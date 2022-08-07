# Testing and Continuous Integration

There are five levels of testing available for the Spark Cyclone plugin codebase,
four of which are automated by Github Actions on `Pull Request Open`.

## C++ Cyclone Library

On a machine with VE hardware and `ncc` available, the test suites for the C++
Cyclone library are built and run as part of running either the JVM tests suite
or the VE tests suite (see below).  Please visit the
[C++ Cyclone Library](../src/main/resources/io/sparkcyclone/cpp/README.md) page
for more information.

## Spark Cyclone Plugin

### JVM Tests

Unit and functional test suites are available that exercise the VE-independent
JVM-level code in the plugin.  To run these tests in `sbt`:

```sbt
// Run all JVM tests
test

// Run a specitic JVM test class
testOnly *PointerOpsUnitSpec*
```

### VE Tests

VE unit and functional test suites are defined in the `VectorEngine` `sbt` scope
and test the interation between the JVM and Spark with the VE itself.  They are
annotated with the `io.sparkcyclone.annotations.VectorEngineTest` annotation, and
as the name implies, they require a VE and `ncc` to be present on the system to
run properly.  To run these tests in `sbt`:

```sbt
// Run all VE tests
VectorEngine / testQuick

// Run a specific VE test class
VectorEngine / testOnly *VeColVectorUnitSpec*
```

### TPC (VE) Tests

The `TPC` `sbt` scope contains test suites that fully exercise the Spark Cyclone
plugin by running SQL queries used in the [TPC-H](https://www.tpc.org/tpch/)
benchmark.  Generally, these tests can be thought of as integration tests for
the plugin itself and have been a valuable tool in discovering bugs in the query
tree manipulation, C++ code generation, and VE execution that the JVM and VE test
suites have not been able to test for.

To run the `TPC` tests, the dataset must first be generated using `dbgen`, a
tool provided by the TPC Consortium:

```sh
# Go to the dbgen directory from project root
$ cd src/test/resources/dbgen/

# Build the dataset generation program
$ make

# Generate the sample dataset (Scale Factor 1, or 1GB, by default)
$ ./dbgen
```

To run the `TPC` tests in `sbt`:

```sbt
// [OPTIONAL] Output verbose details, like plan tree information
set debugToHtml := true

// [OPTIONAL] Fail fast on the first failed query
set failFast := true

// Run the the full TPC test suite (22 queries total)
TPC / testOnly *TPC*VE*

// Run just one query
TPC / testOnly *TPC*VE* -- -z "Query 13"
```

To compare the query results with those that come from a vanilla Spark run of
the benchmark (without the plugin):

```sbt
TPC / testOnly *TPC*JVM*
```

### YARN-Based TPC (VE) Tests

The `TPC` tests are useful tools for plugin development, but their execution is
very slow and, and the setup is not reflective of real world Spark jobs, which
are run through YARN as opposed to `sbt` console.  More importantly, the sample
dataset size is limited to small scale factor before the JVM runs out of memory,
and so bugs that may be triggered only when the plugin is run with large datasets
will be missed.

To address this test gap, a standalone Spark job that embodies the `TPC-H`
benchmark  is provided in `tests/tpchbench`.

To run the YARN-based `TPC` tests, the data must first be generated and pushed
into `hdfs`:

```sh
# From the `tests/tpchbench` directory
$ cd dbgen/

# Generate the sample dataset with Scale Factor 10, or 10GB
$ ./dbgen -s 10

# Switch the user to normal mode if needed
$ /opt/hadoop/bin/hdfs dfsadmin -safemode leave

# Upload the directory with the generated tables into `hdfs`
$ cd .. && /opt/hadoop/bin/hdfs dfs -put dbgen dbgen10
```

Assuming a YARN cluster is accessible, the job can be built and submitted for
execution as follows:

```sh
# From the `tests/tpchbench` directory
$ sbt assembly

# Submit the TPC (VE) job (specify hdfs://~/dbgen10/ as the dataset)
$ ./run_ve.sh --dbgen=dbgen10

# Submit the TPC (vanilla) job (without the plugin)
$ ./run_cpu.sh --dbgen=dbgen10
```

Note that Hadoop requires at least 10% free disk space on the system it is
running on, or the submitted Spark job will loop forever waiting for resources.

The YARN-based `TPC` tests are black-box by design, and while they are not
automated by Github Actions, they should always be run as a final sanity check
when introducing changes to the codebase.
