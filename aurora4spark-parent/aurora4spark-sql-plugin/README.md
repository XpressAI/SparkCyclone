# aurora4spark-sql-plugin

Requirements:

- SBT
- Hadoop
- JDK 8

## Development workflow

Within sbt:

- Unit tests: `~testQuick`
- Acceptance tests: `~ AcceptanceTest / testQuick` (or `Acc`)
- Run once: `Acc/test`
- Run a specific unit test suite: `~ testOnly *SuiteName*`
- Scalafmt format: `fmt`
- Check before committing: `check` (checks scalafmt and runs any outstanding unit tests)
- After adding new query type, when implementing the query tests add `markup([QUERY])`, so that it will automatically
  added to `../FEATURES.md`.

## Currently supported queries

List of currently supported and tested queries can be found [in this file](../FEATURES.md).

## Produce the deployable JAR

```
> show packageBin
[info] C:\...\aurora4spark-sql-plugin_2.12-0.1.0-SNAPSHOT.jar
```

### Deploy the key parts to `a6`

```
> deploy
```

Will upload the `.jar` file and the example `.py` file.

To deploy without running unit tests:

```
> ; set assembly / test := {}; deploy
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


## `ve-direct`

This is the first iteration of using `aurora4j` straight from Scala. This uses direct memory access and all that. 

Assuming `ssh ed hostname` returns `XAIJPVE1`, from SBT run: `ve-direct / IntegrationTest / test`.

