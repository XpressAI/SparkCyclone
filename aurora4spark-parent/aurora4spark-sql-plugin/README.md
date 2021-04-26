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
- After adding new query type, when implementing the query tests add `markup([QUERY])`,
  so that it will automatically added to `../FEATURES.md`.

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
  
### Faster testing over SSH (around 40%) & general log-in to any SSH server

https://docs.rackspace.com/blog/speeding-up-ssh-session-creation/
