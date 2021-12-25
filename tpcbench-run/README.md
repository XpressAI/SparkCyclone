# tpcbench-run

This utility runs individual benchmarks on YARN from a Scala context and saves the results
to `/tmp/benchmark-results.db`.

Key information like cluster and tracking URLs are printed upon start-up for better developer experience.

## Assumptions

- `dbgen` data is on HDFS in the `/user/github/` directory
- `Java 1.8 set up`
- `SBT set up`
- `SPARK_HOME=/opt/spark` is set up. Spark is 3.1.x.

## `benchmark-results.db`

This table has both the input parameters and the key output results stored, including tracing information. To use:

```
$ sqlite3 /tmp/benchmark-results.db -cmd '.mode column' -cmd '.headers on' 'select gitCommitSha,queryNo,scale,wallTime,appUrl,useCyclone from run_result order by id desc limit 3'
gitCommitSha  queryNo     scale       wallTime    appUrl                                                           useCyclone
------------  ----------  ----------  ----------  ---------------------------------------------------------------  ----------
5d75dddc      1           1           84          http://cluster:8088/cluster/app/application_1638487109505_0305  true      
5d75dddc      1           1           106         http://cluster:8088/cluster/app/application_1638487109505_0304  true      
a702c72d      1           1           64          http://cluster:8088/cluster/app/application_1638487109505_0302  false     
```

### Schema

The columns in here are dynamically added based on the definition of `RunOptions` and `RunResult`, so there is no need
to worry about schema evolution.

## Running

```
$ sbt
> tpcbench-run/reStart
$ sbt
> tpcbench-run/reStart --conf spark.ui.enabled=true --conf --extra=spark.ui.port=4055 --query=1
```

### All the options (optional)

| option name        | example                            | purpose                                                                                                     |
|--------------------|------------------------------------|-------------------------------------------------------------------------------------------------------------|
| `query`            | `--query=2`                        | pick which query to run                                                                                     |
| `cyclone`          | `--cyclone=off`                    | pick whether to run in Cyclone plugin or not                                                                |
| `scale`            | `--scale=1`                        | pick which scale to use (`1`, `10`, `20`)                                                                   |
| `name`             | `--name=abc`                       | name of the job                                                                                             |
| `serializer`       | `--serializer=off`                 | whether to use the Cyclone serializer for caching                                                           |
| `ve-log-debug`     | `--ve-log-debug=on`                | whether to set the environment variable for AVEO to debug what it's doing (`VEO_LOG_DEBUG=1`, heavy logging |
| `kernel-directory` | `--kernel-directory=/tmp/my/space` | where to cache compiled `.so` modules                                                                       |
| `--extra`          | `--extra=--conf --extra=abcd`      | append arguments to the `spark-submit` command. Here, it will append `--conf abcd`                          |
| `--conf x`    | `--conf abcd`                      | append arguments to the `spark-submit` command. Here, it will append `--conf abcd`                          |

## All result options

These are subject to change, refer to your instance for completeness.

```
$ sqlite3 /tmp/benchmark-results.db -cmd '.mode column' -cmd '.headers on' 'pragma table_info(run_result);'cid         name        type        notnull     dflt_value  pk        
----------  ----------  ----------  ----------  ----------  ----------
0           id          INTEGER     0                       1         
1           timestamp   DATETIME    0           CURRENT_TI  0         
2           gitCommitS              0                       0         
3           queryNo                 0                       0         
4           name                    0                       0         
5           numExecuto              0                       0         
6           executorCo              0                       0         
7           executorMe              0                       0         
8           scale                   0                       0         
9           offHeapEna              0                       0         
10          columnBatc              0                       0         
11          serializer              0                       0         
12          secondsTak              0                       0         
13          veDebug                 0                       0         
14          runId                   0                       0         
15          succeeded               0                       0         
16          wallTime                0                       0         
17          queryTime               0                       0         
18          traceResul              0                       0         
19          kernelDire              0                       0         
20          veLogDebug              0                       0         
21          codeDebug               0                       0         
22          extras                  0                       0         
23          aggregateO              0                       0         
24          enableVeSo              0                       0         
25          projectOnV              0                       0         
26          filterOnVe              0                       0         
27          exchangeOn              0                       0         
28          appUrl                  0                       0         
29          useCyclone              0                       0         
```
