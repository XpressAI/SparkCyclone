# aurora4spark

Requirements:
- Maven
- Hadoop
- JDK 8+

## Development workflow

- Unit test: `mvn test`
- Unit test + scalastyle: `mvn verify`
- Integration/Acceptance test + unit test: `mvn verify -DskipIntegrationTests=false`
- Integration/Acceptance test w/o unit tests: `mvn verify -DskipUnitTests=true -DskipIntegrationTests=false`
- Scalafmt auto-formatting: `mvn mvn-scalafmt_2.12:format`
### Collaboration

- Make a PR -> Reviewer approves & if happy, merges with rebase
- Aim for 1 commit per distinct feature
