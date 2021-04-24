# aurora4spark

Requirements:
- Maven
- Hadoop
- JDK 8

## Development workflow

Like Spark, we separate development workflow from general building.

For development workflow with SBT, go to: [aurora4spark-sql-plugin/README.md](aurora4spark-sql-plugin/README.md). SBT is used because of faster turnaround times in TDD.

Nonetheless, these are the relevant options in Maven:

- Unit test: `mvn test`
- Unit test + scalastyle: `mvn verify`
- Integration/Acceptance test + unit test: `mvn verify -DskipIntegrationTests=false`
- Integration/Acceptance test w/o unit tests: `mvn verify -DskipUnitTests=true -DskipIntegrationTests=false`
- Run specific integration test: `mvn integration-test '-DwildcardSuites=com.nec.spark.agile.BigDecimalSummerSpec' '-DskipIntegrationTests=false'`
- Scalafmt auto-formatting: `mvn mvn-scalafmt_2.12:format`


### Collaboration

- Make a PR -> Reviewer approves & if happy, merges with rebase
- Aim for 1 commit per distinct feature

