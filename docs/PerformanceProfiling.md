# Performance Profiling

JVM-level performance profiling for the Spark Cyclone plugin can be achieved
with the use of [Java Flight Recorder](https://docs.oracle.com/javacomponents/jmc-5-4/jfr-runtime-guide/about.htm#JFRUH170),
which comes with the JDK.

### Hadoop Configuration

We need to be able to collect the JFR files after a Spark run, which are
stored in the `appcache` directory corresponding to the Hadoop job.  By default,
this directory is immediately cleared after the job completes.  The following
must be added to the Hadoop configuration (`/opt/hadoop/etc/hadoop/yarn-site.xml`)
so that the `appcache` data is retained for a set amount of time after job
completion:

```xml
    <property>
        <name>yarn.nodemanager.delete.debug-delay-sec</name>
        <value>600</value>
    </property>
```

The Hadoop cluster needs to be restarted (as user `hadoop`) after the
configuration is set:

```sh
# Shut down the cluster
$ su hadoop /opt/hadoop/sbin/stop-yarn.sh
$ su hadoop /opt/hadoop/sbin/stop-dfs.sh

# Restart the cluster
$ su hadoop /opt/hadoop/sbin/start-yarn.sh
$ su hadoop /opt/hadoop/sbin/start-dfs.sh
```

Note that the shutdown scripts may run successfully without actually shutting
down the cluster, so it may be useful to verify this with the `jps` command and
kill directly with `kill -9`:

```sh
$ jps
31489 ResourceManager
34161 SecondaryNameNode
33378 NameNode
31818 NodeManager
33663 DataNode
```

After restart, a switch back to HDFS normal mode will be needed:

```sh
$ su hadoop -- /opt/hadoop/bin/hdfs dfsadmin -safemode leave
```

### Plugin Configuration

Copy the [JFR settings file](../src/main/resources/profiling/settings.jfc)
into a location that can be referenced by the Spark job.  In the Spark job
configuration, add the following two lines:

```sh
JFC_SETTINGS=/path/to/settings.jfc

--conf spark.driver.extraJavaOptions="-XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:StartFlightRecording=duration=600s,settings=$JFC_SETTINGS, filename=driver_events.jfr"
--conf spark.executor.extraJavaOptions="-XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:StartFlightRecording=duration=600s,settings=$JFC_SETTINGS,filename=executor.jfr"
```

### Profile Report Collection

After the job completes, the JFR output files will be found in the `appcache`
directory corresponding to the Spark job.  They can be copied over to the
current working directory as follows:

```sh
USER=# The user that kicked off the Spark job
JOB_ID=# The Spark job ID

# Copy the files over
for d in `find /home/hadoop/nm-local-dir/usercache/$USER/appcache/$JOB_ID -iname '*.jfr' | grep -v tmp | xargs dirname`; do cp $d/executor.jfr `basename $d`.jfr ; done
```

There will be one JFR file corresponding to each container that ran the job
(including the driver).
