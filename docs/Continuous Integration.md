# Continuous Integration

Automated testing is done using GitHub Actions and ScalaTest over SBT.

The key tests that are available in Cyclone for Actions are:

- Unit test suites (pure JVM level), to validate simple logic, independent of platform used, so they can be ran with
  minimal set up. This is usually for some simpler logic validation.
- `VectorEngine` scope test suites, to validate, at unit and component level, the interaction between the JVM and the
  Vector Engine through the native layers. These require you to be running on a Vector Engine machine. More detail how
  to set it up is in `/SBT.md`.
- `TPC` scope test suites, which are to run TPC-H end-to-end functional tests on real-world data-sets using TPC data.
  These are still done from within the plug-in, fairly white-box, and can be used as part of a developer workflow. You
  should ideally be working from unit to VE to TPC scopes, not the other way round, to follow the test pyramid.
- Black-box TPC tests that run on YARN. These are available as a one-shot run (`benchmark-collector-all.yml` to run all
  of the 20+ queries) and specialized run (`benchmark-collector.yml` to run any specific test you are interested in).
  These are highly customizable and also save results of the runs into a file which is available on `/tpc-html/summary/`
  on the Cyclone website to enable a clear understanding of the shape of the project (the `tpcbench-run` project).
