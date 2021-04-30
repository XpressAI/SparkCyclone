# aveo4j

Repository for the Java interface to Aveo, based on JavaCPP.

https://github.com/SX-Aurora/aveo/

## Deployment to `../aveo4j-repo`

Currently, this library is released into the local file-based Maven repository at `../aveo4j-repo`, for quick
consumption by the Spark plug-in.

After modifying the code, to redeploy to `../aveo4j-repo`, use `mvn clean deploy`, and then commit the changes.

We can move to use a proper Maven repository scheme later, however this is the simplest for now as 0 new infra is
needed.
