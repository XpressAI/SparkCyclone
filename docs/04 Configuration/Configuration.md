# Aurora4Spark Configuration

Basic configuration to run spark job in Vector Engine: 

```bash
$SPARK_HOME/bin/spark-submit \
	--master yarn \
	--num-executors=8 --executor-cores=1 --executor-memory=7G \
	--name job \
	--conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
	--jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
	--conf spark.executor.extraClassPath=/opt/aurora4spark/aurora4spark-sql-plugin.jar \
	--conf spark.plugins=com.nec.spark.AuroraSqlPlugin \
	--conf spark.driver.resource.ve.discoveryScript=/opt/spark/getVEsResources.sh \
	--conf spark.executor.resource.ve.amount=1 \
	--conf spark.executor.resource.ve.discoveryScript=/opt/spark/getVEsResources.sh \
	job.py
```



## General Configuration

| Name                                                 | Description                                                  | Default Value             |
| ---------------------------------------------------- | :----------------------------------------------------------- | ------------------------- |
| spark.com.nec.spark.ncc.path                         | Specifying ncc path. Please specify the absolute path if ncc is not in your `$PATH` | ncc                       |
| spark.com.nec.spark.ncc.debug                        | ncc debug mode                                               | false                     |
| spark.com.nec.spark.ncc.o                            | Optimization level for Vector Engine Compiler                | 4                         |
| spark.com.nec.spark.ncc.openmp                       | Use openMP                                                   | false                     |
| spark.com.nec.spark.ncc.extra-argument.0             | Additional options for Vector Engine Compiler. For example: "-X" | ""                        |
| spark.com.nec.spark.ncc.extra-argument.1             | Additional options for Vector Engine Compiler: For example: "-Y" | ""                        |
| spark.com.nec.native-csv                             | Native CSV parser. Available options: "x86" : uses `CNativeEvaluator`, "ve": uses `ExecutorPluginManagedEvaluator` | off                       |
| spark.com.nec.native-csv-ipc                         | Using IPC for parsing CSV. Spark -> IPC -> VE CSV            | true                      |
| spark.com.nec.native-csv-skip-strings                | To use String allocation as opposed to ByteArray optimization in `NativeCsvExec`, set it to false. | true                      |
| spark.executor.resource.ve.amount                    | This is definitely needed. For example: "1"                  | -                         |
| spark.task.resource.ve.amount                        | Not clear if this is needed, For example: "1"                | -                         |
| spark.worker.resource.ve.amount                      | This seems to be necessary for cluster-local mode, For example: "1" | -                         |
| spark.resources.discoveryPlugin                      | Detecting resources automatically. Set it to `com.nec.ve.DiscoverVectorEnginesPlugin` to enable it | -                         |
| spark.[executor\|driver].resource.ve.discoveryScript | Specifying resources via file. Set it to `/opt/spark/getVEsResources.sh` to enable it or where ever your script is located | -                         |
| spark.com.nec.spark.kernel.precompiled               | Use a precompiled directory                                  | -                         |
| spark.com.nec.spark.kernel.directory                 | If precompiled directory is not yet exist, then you can also specify a destination for on-demand compilation. If this is not specified, then a random temporary directory will be used (not removed, however). | random temporay directory |
| spark.com.nec.spark.batch-batches                    | This is to batch ColumnarBatch together, to allow for larger input sizes into the VE. This may however use more on-heap and off-heap memory. | 0                         |
| com.nec.spark.preshuffle-partitions                  | Avoids a coalesce into a single partition, trading it off for pre-sorting/pre-partitioning data by hashes of the group-by expressions | -                         |

For `spark.com.nec.spark.ncc.extra-argument.[0-?]`. Please refer https://www.hpc.nec/documents/sdk/pdfs/g2af01e-C++UsersGuide-023.pdf