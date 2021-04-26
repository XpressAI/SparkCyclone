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
  so that it will automatically added to this README.
### Faster testing over SSH (around 40%) & general log-in to any SSH server

https://docs.rackspace.com/blog/speeding-up-ssh-session-creation/

## Currently supported queries
List of currently supported and tested queries can be found [in this file](../FEATURES.md).